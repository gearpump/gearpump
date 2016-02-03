/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

/**
 * call rest service /login to setup login session tokens.
 * If login succeeds, it will redirect to dashboard home page.
 */
function login() {

    var loginUrl = $("#loginUrl").attr('href');
    var index = $("#index").attr('href');

    $.post(loginUrl, $("#loginForm").serialize() ).done(
      function(msg) {
          var user = $.parseJSON(msg);
          $.cookie("username", user.user, { expires: 365, path: '/' });
          // clear the errors
          $("#error").text("");
          // redirect to index.html
          $(location).attr('href', index);
      }
    )
    .fail( function(xhr, textStatus, errorThrown) {
       var elem = $("#error");
            elem.html(xhr.responseText);
            elem.text(textStatus + "(" + xhr.status + "): " + elem.text());
    });
}

/**
 * call rest service /logout to clear the session tokens.
 */
function logout() {
    var logoutUrl = $("#logoutUrl").attr('href');
    $.post(logoutUrl)
}

$(document).ready(function() {
    // Send a initial logout to clear the sessions.
    logout();
});