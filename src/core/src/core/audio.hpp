#pragma once

#include <boost/asio.hpp>
#define BOOST_THREAD_PROVIDES_FUTURE_CONTINUATION
#define BOOST_THREAD_PROVIDES_FUTURE
#include <boost/thread.hpp>
#include <boost/thread/future.hpp>
#include <functional>
#include <memory>
#include <string>
#include <vector>

struct pa_context;

namespace wolf::core::audio {

typedef struct Server Server;

pa_context *context(const std::shared_ptr<Server> &server);

std::shared_ptr<Server> connect(std::string_view server = {});

constexpr auto SAMPLE_RATE = 48000;

struct AudioMode {

  enum class Speakers {
    FRONT_LEFT,
    FRONT_RIGHT,
    FRONT_CENTER,
    LOW_FREQUENCY,
    BACK_LEFT,
    BACK_RIGHT,
    SIDE_LEFT,
    SIDE_RIGHT,
    MAX_SPEAKERS,
  };

  int channels{};
  int streams{};
  int coupled_streams{};
  std::vector<Speakers> speakers;
  int bitrate{};
  int sample_rate = SAMPLE_RATE;
};

struct AudioDevice {
  std::string_view sink_name;
  AudioMode mode;
  /** Human readable description shown in PulseAudio UIs (optional). */
  std::string_view description{};
};

struct VSink {
  AudioDevice device;
  boost::promise<unsigned int> sink_idx;
};

/**
 * A loaded PulseAudio `module-loopback`. Used to bridge one virtual microphone
 * (source, usually `<sink>.monitor`) into another virtual microphone (sink)
 * without touching the GStreamer pipeline that feeds the source.
 */
struct VLoopback {
  boost::promise<unsigned int> module_idx;
};

/**
 * Will sit here and wait until the server accepts the connection or some error happens
 */
bool connected(const std::shared_ptr<Server> &server);

void queue_op(const std::shared_ptr<Server> &server, const std::function<void()> &op);

std::shared_ptr<VSink> create_virtual_sink(const std::shared_ptr<Server> &server, const AudioDevice &device);

void delete_virtual_sink(const std::shared_ptr<Server> &server, const std::shared_ptr<VSink> &vsink);

/**
 * Loads a PulseAudio `module-loopback` forwarding audio from `source_name` to `sink_name`.
 *
 * This is used to feed a lobby's virtual microphone from a session's virtual microphone
 * while keeping the microphone receiver's GStreamer pipeline (and its `pulsesink`)
 * pinned to the session sink for the whole session: changing the `device` property of a
 * live `pulsesink` breaks negotiation ("Sink not negotiated before eos event").
 */
std::shared_ptr<VLoopback> create_loopback(const std::shared_ptr<Server> &server,
                                           std::string_view source_name,
                                           std::string_view sink_name);

void delete_loopback(const std::shared_ptr<Server> &server, const std::shared_ptr<VLoopback> &vloopback);

/**
 * Sets the server-wide default recording source (e.g. `virtual_mic_<id>.monitor`).
 *
 * `PULSE_SOURCE` only affects clients that honour it; some applications (notably Steam) record
 * from the server default source instead, so the virtual microphone must also be selected here.
 */
void set_default_source(const std::shared_ptr<Server> &server, std::string_view source_name);

void disconnect(const std::shared_ptr<Server> &server);

std::string get_server_name(const std::shared_ptr<Server> &server);

} // namespace wolf::core::audio
