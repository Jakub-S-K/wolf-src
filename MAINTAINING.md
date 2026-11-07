# Maintaining this fork (Wolf + microphone)

This fork adds **microphone support** to Wolf using the *Foundation Sunshine* mic
protocol, so a client (native Moonlight or the web UI) can send its microphone and
Wolf exposes it as a virtual microphone inside the game container
(`virtual_mic_<session>.monitor`, description `WolfMicrophone`).

All our work lives in small commits on top of upstream. Keeping them as a linear
series makes it easy to rebase onto new upstream releases.

## Branch layout
- `feat/microphone` – our commits, based on upstream `stable`
  (pinned at `d6d41dec`, the revision of the `wolf:stable` image we started from).
- `upstream` – remote pointing at `https://github.com/games-on-whales/wolf.git`.

Our commits (oldest first):
1. `feat(serverinfo): advertise Foundation Sunshine microphone support`
2. `feat(audio): named virtual sinks and virtual microphone backend`
3. `feat(mic): receive Foundation microphone and play it into a virtual mic`
4. `feat(mic): route microphone through lobbies and keep it on resume`
5. `chore(gstreamer): include debug details in pipeline error logs`

## One-time setup (your GitHub fork)
```bash
git remote add upstream https://github.com/games-on-whales/wolf.git
git remote add origin   git@github.com:<YOUR_USER>/wolf.git   # your fork
git push -u origin feat/microphone
```
Create the empty fork on GitHub first (do **not** initialise it with a README).

## Updating from upstream
```bash
git fetch upstream
git checkout feat/microphone
git rebase upstream/stable        # resolve conflicts if upstream touched the same files
git push --force-with-lease origin feat/microphone
```
Rebasing keeps our five commits on top and avoids merge bubbles. If you prefer
merges: `git merge upstream/stable`.

## Building
The image is built on the Proxmox host (x86_64):
```bash
docker build -f docker/wolf.Dockerfile -t wolf:mic .
```
Then restart the container with your launcher (the reference launcher is
`/root/rdocker.mic` on the Proxmox host; it is the original `docker run` command
with `-d` so it does not block).

## Testing the microphone
- `/serverinfo` must contain `<SunshineVersion>` (Foundation detection).
- On launch, logs should show `[MIC] Microphone socket bound to port …` and
  `[MIC] Starting microphone receiver …`.
- Playback target: `virtual_mic_<session>` / `virtual_mic_<lobby>`;
  the app container gets `PULSE_SOURCE=virtual_mic_<id>.monitor`.
- Quick check:
  ```bash
  docker exec wolf sh -c \
    'PULSE_SERVER=/tmp/sockets/pulse-socket pactl list short sink-inputs'
  ```
  a `48 kHz` input on a `virtual_mic_*` sink means microphone audio is flowing.

## Known issues / follow-ups
- The lobby audio producer (the `pulsesrc` recording `virtual_sink_<lobby>.monitor`)
  can die after a player joins/leaves a lobby and is not recreated; the stream then
  switches to a silent lobby feed. A watchdog that recreates the lobby producer
  would fix it.
- `SwitchMicSinkEvent` currently changes the receiver's `pulsesink` device with
  `g_object_set`; switching can leave `pulsesink` in a bad state. Prefer recreating
  the microphone pipeline (or use a `module-loopback`) on sink switches.
- Sink descriptions passed to `module-null-sink` must not contain spaces
  (`device.description=WolfMicrophone`, not `Wolf Microphone`).
