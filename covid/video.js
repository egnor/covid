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

        function play_pause() {
            if (key_target) key_target.focus();
            if (video.currentTime > video.duration - 0.2) rewind();
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
            video.pause()
            video.currentTime = 0;
        }

        function prev_frame() {
            if (key_target) key_target.focus();
            if (prev_button) prev_button.classList.add('seeking');
            video.pause();
            video.currentTime = (Math.ceil(video.currentTime * 3) - 1.2) / 3;
        }

        function next_frame() {
            if (key_target) key_target.focus();
            if (next_button) next_button.classList.add('seeking');
            video.pause();
            video.currentTime = (Math.floor(video.currentTime * 3) + 1.2) / 3;
        }

        function forward() {
            if (key_target) key_target.focus();
            if (isFinite(video.duration)) {
                if (forward_button) forward_button.classList.add('seeking');
                video.pause();
                video.currentTime = video.duration;
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

        var seek_timeout_id;
        video.addEventListener('seeked', function() {
            if (seek_timeout_id) window.clearTimeout(seek_timeout_id);
            seek_timeout_id = window.setTimeout(function() {
                seek_timeout_id = null;
                if (rewind_button) rewind_button.classList.remove('seeking');
                if (prev_button) prev_button.classList.remove('seeking');
                if (next_button) next_button.classList.remove('seeking');
                if (forward_button) forward_button.classList.remove('seeking');
            }, 150);
        });

        if (loop_button) loop_button.addEventListener('click', loop_toggle);
        if (rewind_button) rewind_button.addEventListener('click', rewind);
        if (prev_button) prev_button.addEventListener('click', prev_frame);
        if (next_button) next_button.addEventListener('click', next_frame);
        if (forward_button) forward_button.addEventListener('click', forward);

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
