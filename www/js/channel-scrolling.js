// Controls scrolling of channel shows 


    // Channel/track 1

    setInterval(function() {

      var speed = 3;


      // Enlarge show widths if the window is wide
      // Scale with width of window
      if($(window).width() > 645) {
        $('.show').css('width', $(window).width()*0.15);
      }

      $('.track').css('width', $('.show').width() * $('#track1').children().length + 100)




      var left = parseInt($('#track1').css('left'));
      
      if ($('#left1').is(":hover")) {
        if($('#track1').position().left <= 5) {
          $('#track1').css('left', left+speed);
        }
      }
      else if ($('#right1').is(":hover")) {
        if($('#track1').position().left >= -$('.show').width() + $(window).width() - $('.show').width() * $('#track1').children().length) {
          $('#track1').css('left', left-speed);
        }
      }


      if($('#track1').position().left > 5) {
        $('#left1').hide();
      } else {
        $('#left1').show();
      }

      if($('#track1').position().left < -$('.show').width() + $(window).width() - $('.show').width() * $('#track1').children().length) {
        $('#right1').hide();
      } else {
        $('#right1').show();
      }



      // Channel/track 2

      var left = parseInt($('#track2').css('left'));
      
      if ($('#left2').is(":hover")) {
        if($('#track2').position().left <= 5) {
          $('#track2').css('left', left+speed);
        }
      }
      else if ($('#right2').is(":hover")) {
        if($('#track2').position().left >= -$('.show').width() + $(window).width() - $('.show').width() * $('#track2').children().length) {
          $('#track2').css('left', left-speed);
        }
      }


      if($('#track2').position().left > 5) {
        $('#left2').hide();
      } else {
        $('#left2').show();
      }

      if($('#track2').position().left < -$('.show').width() + $(window).width() - $('.show').width() * $('#track2').children().length) {
        $('#right2').hide();
      } else {
        $('#right2').show();
      }

    }, 10);