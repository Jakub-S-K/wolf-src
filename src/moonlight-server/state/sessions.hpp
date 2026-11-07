#pragma once

#include <cerrno>
#include <cstring>
#include <events/events.hpp>
#include <helpers/logger.hpp>
#include <helpers/utils.hpp>
#include <immer/vector.hpp>
#include <memory>
#include <netinet/in.h>
#include <optional>
#include <range/v3/view.hpp>
#include <state/config.hpp>
#include <state/serialised_config.hpp>
#include <sys/socket.h>
#include <unistd.h>

namespace state {

using namespace wolf::core;

/**
 * Opens a UDP socket bound to an ephemeral local port and returns it together with the
 * assigned port. Used for the Foundation microphone extension: the client sends RTP/Opus
 * to a port the server picks. We use a distinct port per session because all microphone
 * packets may arrive from the same relay/client IP (e.g. moonlight-web-stream), so they
 * cannot be demultiplexed by source address alone.
 */
inline std::shared_ptr<int> open_mic_socket(unsigned short &out_port) {
  out_port = 0;
  int fd = ::socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    logs::log(logs::warning, "[MIC] Unable to create microphone socket: {}", std::strerror(errno));
    return nullptr;
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = 0;
  if (::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    logs::log(logs::warning, "[MIC] Unable to bind microphone socket: {}", std::strerror(errno));
    ::close(fd);
    return nullptr;
  }

  socklen_t addr_len = sizeof(addr);
  if (::getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &addr_len) < 0) {
    logs::log(logs::warning, "[MIC] Unable to read microphone socket port: {}", std::strerror(errno));
    ::close(fd);
    return nullptr;
  }
  out_port = ntohs(addr.sin_port);
  logs::log(logs::debug, "[MIC] Microphone socket bound to port {}", out_port);

  return std::shared_ptr<int>(new int(fd), [](int *p) {
    if (p) {
      if (*p >= 0) {
        ::close(*p);
      }
      delete p;
    }
  });
}

inline std::optional<events::StreamSession> get_session_by_id(const immer::vector<events::StreamSession> &sessions,
                                                              const std::size_t id) {
  auto results =
      sessions |                                                                                             //
      ranges::views::filter([id](const events::StreamSession &session) { return session.session_id == id; }) //
      | ranges::views::take(1)                                                                               //
      | ranges::to_vector;                                                                                   //
  if (results.size() == 1) {
    return results[0];
  } else if (results.empty()) {
    return {};
  } else {
    logs::log(logs::warning, "Found multiple sessions for a given ID: {}", id);
    return {};
  }
}

inline std::optional<events::StreamSession> get_session_by_client(const immer::vector<events::StreamSession> &sessions,
                                                                  const wolf::config::PairedClient &client) {
  auto client_id = get_client_id(client);
  return get_session_by_id(sessions, client_id);
}

inline std::optional<events::Lobby> get_lobby_by_id(const immer::vector<events::Lobby> &lobbies,
                                                    std::string_view lobby_id) {
  auto results = lobbies |                                                                                      //
                 ranges::views::filter([lobby_id](const events::Lobby &lobby) { return lobby.id == lobby_id; }) //
                 | ranges::views::take(1)                                                                       //
                 | ranges::to_vector;                                                                           //
  if (results.size() == 1) {
    return results[0];
  } else if (results.empty()) {
    return {};
  } else {
    logs::log(logs::warning, "Found multiple lobbies for a given ID: {}", lobby_id);
    return {};
  }
}

inline std::optional<events::Lobby> get_lobby_by_connected_session(const immer::vector<events::Lobby> &lobbies,
                                                                   std::string_view session_id) {
  for (const events::Lobby &lobby : lobbies) {
    immer::vector<immer::box<std::string>> sessions = lobby.connected_sessions->load();
    auto session = std::find_if(sessions.begin(), sessions.end(), [session_id](const auto &session) {
      return session == session_id;
    });
    if (session == sessions.end()) {
      continue;
    }
    return lobby;
  }
  return {};
}

inline std::shared_ptr<events::StreamSession> create_stream_session(immer::box<state::AppState> state,
                                                                    const events::App &run_app,
                                                                    const wolf::config::PairedClient &current_client,
                                                                    const moonlight::DisplayMode &display_mode,
                                                                    int audio_channel_count,
                                                                    const std::string &aes_key,
                                                                    const std::string &aes_iv) {
  auto full_path = std::filesystem::path(state->host->local_base_state_folder) / current_client.app_state_folder /
                   run_app.base.title;
  logs::log(logs::debug, "Host app state folder: {}, creating paths", full_path.string());
  std::filesystem::create_directories(full_path);

  std::random_device rd;
  std::mt19937 generator(rd());

  std::uniform_int_distribution<> chars(33, 126); // ASCII values for printable character
  std::array<char, 16> rtp_secret_payload;
  for (auto &c : rtp_secret_payload) {
    c = static_cast<char>(chars(generator));
  }

  std::uniform_int_distribution<u_int32_t> uints(0, UINT32_MAX);

  std::uniform_int_distribution<> ints(0, 255);
  auto rtsp_fake_ip = fmt::format("{}.{}.{}.{}", ints(generator), ints(generator), ints(generator), ints(generator));

  unsigned short mic_stream_port = 0;
  auto mic_socket_fd = open_mic_socket(mic_stream_port);

  auto session = events::StreamSession{
      .display_mode = display_mode,
      .audio_channel_count = audio_channel_count,
      .event_bus = state->event_bus,
      .client_settings = current_client.settings,
      .app = std::make_shared<events::App>(run_app),
      .app_local_state_folder = full_path.string(),
      .app_host_state_folder = std::filesystem::path(state->host->host_base_state_folder) /
                               current_client.app_state_folder / run_app.base.title,

      .aes_key = aes_key,
      .aes_iv = aes_iv,

      // Moonlight protocol extension to support IP-less connections
      .rtp_secret_payload = rtp_secret_payload,
      .enet_secret_payload = uints(generator),
      .rtsp_fake_ip = rtsp_fake_ip,

      // client info
      .session_id = get_client_id(current_client),
      .video_stream_port = static_cast<unsigned short>(get_port(VIDEO_PING_PORT)),
      .audio_stream_port = static_cast<unsigned short>(get_port(AUDIO_PING_PORT)),
      .control_stream_port = static_cast<unsigned short>(get_port(CONTROL_PORT)),
      .mic_stream_port = mic_stream_port,
      .mic_socket_fd = mic_socket_fd};

  return std::make_shared<events::StreamSession>(session);
}

inline immer::vector<events::StreamSession> remove_session(const immer::vector<events::StreamSession> &sessions,
                                                           const events::StreamSession &session) {
  return sessions                                                                                           //
         | ranges::views::filter([remove_hash = session.session_id](const events::StreamSession &cur_ses) { //
             return cur_ses.session_id != remove_hash;                                                      //
           })                                                                                               //
         | ranges::to<immer::vector<events::StreamSession>>();                                              //
}
} // namespace state