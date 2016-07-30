/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * call rest service /login to setup login session tokens.
 * If login succeeds, it will redirect to dashboard home page.
 */
function login() {

  var loginUrl = $("#loginUrl").attr('href');
  var index = $("#index").attr('href');

  $.post(loginUrl, $("#loginForm").serialize()).done(
    function (msg) {
      var user = $.parseJSON(msg);
      // clear the errors
      $("#error").text("");
      // redirect to index.html
      $(location).attr('href', index);
    }
  )
    .fail(function (xhr, textStatus, errorThrown) {
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

function displaySocialLoginIcons() {
  var loginUrl = $("#loginUrl").attr('href');
  var oauth2Root = loginUrl + "/oauth2";
  var providersUrl = oauth2Root + "/providers";

  var socialLogin = $("#social_login");

  $.get(providersUrl).done(
    function (msg) {
      var providers = $.parseJSON(msg);
      console.log(providers);

      var body = "";

      for (var provider in providers) {
        var icon = providers[provider];
        body += "<a href=" + oauth2Root + "/" + provider + "/" + "authorize>";
        body += "<img src=" + icon + " alt=" + provider + "/> ";
        body += "</a>";
      }

      if (body != "") {
        body = "Social login: " + body
      }

      socialLogin.html(body);
    }
  )
}

$(document).ready(function () {
  // Send a initial logout to clear the sessions.
  logout();

  // Fetch and display social login icons.
  displaySocialLoginIcons()
});
