// Utility JavaScript for COVID pages.

window.addEventListener('load', function(event) {
    var videos = document.getElementsByTagName("VIDEO");
    for (var i = 0; i < videos.length; ++i) {
        var video = videos[i];
        var id = video.id;
        var key_target = id && document.getElementById(id + '_key_target');
        var play_button = id && document.getElementById(id + '_play');
        var loop_button = id && document.getElementById(id + '_loop');
        var rewind_button = id && document.getElementById(id + '_rewind');
        var prev_button = id && document.getElementById(id + '_prev');
        var next_button = id && document.getElementById(id + '_next');
        var forward_button = id && document.getElementById(id + '_forward');
        var slider_range = id && document.getElementById(id + '_slider');

        var seek_active, seek_pending;
        function current_time() {
            return seek_active != null ? seek_pending : video.currentTime;
        }

        function seek_nicely(time) {
            if (slider_range) slider_range.value = time;
            if (seek_active != null) return (seek_pending = time);
            video.pause();
            video.currentTime = seek_active = seek_pending = time;
            window.setTimeout(function() {
                var changed = (seek_pending != seek_active);
                seek_active = null;
                if (changed) return seek_nicely(seek_pending);
                if (rewind_button) rewind_button.classList.remove('seeking');
                if (prev_button) prev_button.classList.remove('seeking');
                if (next_button) next_button.classList.remove('seeking');
                if (forward_button) forward_button.classList.remove('seeking');
            }, 100);
            return time;
        }

        function play_pause() {
            if (key_target) key_target.focus();
            if (current_time() > video.duration - 0.2) rewind();
            (video.paused || video.ended) ? video.play() : video.pause();
        }

        function loop_toggle() {
            if (key_target) key_target.focus();
            var on = (video.loop = !video.loop);
            on ? video.play() : video.pause();
            if (loop_button && !on) loop_button.classList.remove('looping');
            if (loop_button && on) loop_button.classList.add('looping');
        }

        function rewind() {
            if (key_target) key_target.focus();
            if (rewind_button) rewind_button.classList.add('seeking');
            seek_nicely(0);
        }

        function prev_frame() {
            if (key_target) key_target.focus();
            if (prev_button) prev_button.classList.add('seeking');
            var prev = (Math.ceil(current_time() * 3) - 1.5) / 3;
            seek_nicely(prev > 0.2 ? prev : 0);
        }

        function next_frame() {
            if (key_target) key_target.focus();
            if (next_button) next_button.classList.add('seeking');
            var next = (Math.floor(current_time() * 3) + 1.5) / 3;
            seek_nicely(next < video.duration - 0.2 ? next : video.duration);
        }

        function forward() {
            if (key_target) key_target.focus();
            if (isFinite(video.duration)) {
                if (forward_button) forward_button.classList.add('seeking');
                seek_nicely(video.duration);
            }
        }

        video.addEventListener('click', play_pause);

        if (play_button) {
            play_button.addEventListener('click', play_pause);
            video.addEventListener('play', function() {
                play_button.classList.add('playing');
                play_button.classList.remove('paused');
            });
            video.addEventListener('pause', function() {
                play_button.classList.add('paused');
                play_button.classList.remove('playing');
            });
            play_button.classList.add(video.paused ? 'paused' : 'playing');
        }

        if (loop_button) loop_button.addEventListener('click', loop_toggle);
        if (rewind_button) rewind_button.addEventListener('click', rewind);
        if (prev_button) prev_button.addEventListener('click', prev_frame);
        if (next_button) next_button.addEventListener('click', next_frame);
        if (forward_button) forward_button.addEventListener('click', forward);

        if (slider_range) {
            slider_range.addEventListener('input', function() {
                seek_nicely(slider_range.value);
            });

            function slider_from_video() {
                if (!isFinite(video.duration)) return;
                slider_range.min = 0;
                slider_range.max = video.duration;
                slider_range.step = 'any';
                slider_range.value = current_time();
            }
            video.addEventListener('durationchange', slider_from_video);
            video.addEventListener('timeupdate', slider_from_video);
            slider_from_video();
        }

        if (key_target) {
            key_target.tabIndex = (key_target != document.body) ? 0 : -1;
            key_target.focus();
            key_target.addEventListener('keydown', function(event) {
                switch (event.code) {
                    case 'KeyP': play_pause(); break;
                    case 'KeyL': loop_toggle(); break;
                    case 'KeyR': rewind(); break;
                    case 'BracketLeft': prev_frame(); break;
                    case 'BracketRight': next_frame(); break;
                    case 'KeyF': forward(); break;
                }
            });
        }
    }
});
