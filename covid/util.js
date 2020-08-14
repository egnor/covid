// Utility JavaScript for COVID pages.

window.addEventListener('load', function(event) {
  var tags = document.getElementsByTagName("VIDEO");
  for (var i = 0; i < tags.length; ++i) {
    tags[i].addEventListener('keydown', function(event) {
      var video = event.currentTarget;
      if (event.code == 'ArrowLeft') {
        video.pause();
        video.currentTime -= 1 / 3;
        event.preventDefault();
      } else if (event.code == 'ArrowRight') {
        video.pause();
        video.currentTime += 1 / 3;
        event.preventDefault();
      } else if (event.code == 'KeyL') {
        video.loop = video.paused || !video.loop;
        video.loop ? video.play() : video.pause();
        event.preventDefault();
      }
    });
  }
});
