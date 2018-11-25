## What is this about?

## How to enable UI authentication?

1. Change config file gear.conf, find entry `gearpump-ui.gearpump.ui-security.authentication-enabled`, change the value to true

    	:::bash	
    	gearpump-ui.gearpump.ui-security.authentication-enabled = true
    	
   
    Restart the UI dashboard, and then the UI authentication is enabled. It will prompt for user name and password.

## How many authentication methods Gearpump UI server support?

Currently, It supports:

1. Username-Password based authentication and
2. OAuth2 based authentication.

User-Password based authentication is enabled when `gearpump-ui.gearpump.ui-security.authentication-enabled`,
 and **CANNOT** be disabled.

UI server admin can also choose to enable **auxiliary** OAuth2 authentication channel.

## User-Password based authentication

   User-Password based authentication covers all authentication scenarios which requires
   user to enter an explicit username and password.

   Gearpump provides a built-in ConfigFileBasedAuthenticator which verify user name and password
   against password hashcode stored in config files.

   However, developer can choose to extends the `io.gearpump.security.Authenticator` to provide a custom
   User-Password based authenticator, to support LDAP, Kerberos, and Database-based authentication...

### ConfigFileBasedAuthenticator: built-in User-Password Authenticator

ConfigFileBasedAuthenticator store all user name and password hashcode in configuration file gear.conf. Here
is the steps to configure ConfigFileBasedAuthenticator.

#### How to add or remove user?

For the default authentication plugin, it has three categories of users: admins, users, and guests.

* admins: have unlimited permission, like shutdown a cluster, add/remove machines.
* users: have limited permission to submit an application and etc..
* guests: can not submit/kill applications, but can view the application status.

System administrator can add or remove user by updating config file `conf/gear.conf`.  

Suppose we want to add user jerry as an administrator, here are the steps:
  
1. Pick a password, and generate the digest for this password. Suppose we use password `ilovegearpump`, 
   to generate the digest:
   
    	:::bash
    	bin/gear io.gearpump.security.PasswordUtil -password  ilovegearpump
    
   
    It will generate a digest value like this:
   
    	:::bash
    	CgGxGOxlU8ggNdOXejCeLxy+isrCv0TrS37HwA==
    

2. Change config file conf/gear.conf at path `gearpump-ui.gearpump.ui-security.config-file-based-authenticator.admins`,
   add user `jerry` in this list:
   
    	:::bash
	    admins = {
	      ## Default Admin. Username: admin, password: admin
	      ## !!! Please replace this builtin account for production cluster for security reason. !!!
	      "admin" = "AeGxGOxlU8QENdOXejCeLxy+isrCv0TrS37HwA=="
	      "jerry" = "CgGxGOxlU8ggNdOXejCeLxy+isrCv0TrS37HwA=="
	    }
    

3. Restart the UI dashboard by `bin/services` to make the change effective.

4. Group "admins" have very unlimited permission, you may want to restrict the permission. In that case 
   you can modify `gearpump-ui.gearpump.ui-security.config-file-based-authenticator.users` or
   `gearpump-ui.gearpump.ui-security.config-file-based-authenticator.guests`.

5. See description at `conf/gear.conf` to find more information.   
   
#### What is the default user and password?

For ConfigFileBasedAuthenticator, Gearpump distribution is shipped with two default users:

1. username: admin, password: admin
2. username: guest, password: guest

User `admin` has unlimited permissions, while `guest` can only view the application status.

For security reason, you need to remove the default users `admin` and `guest` for cluster in production.

#### Is this secure?

Firstly, we will NOT store any user password in any way so only the user himself knows the password. 
We will use one-way hash digest to verify the user input password.

### How to develop a custom User-Password Authenticator for LDAP, Database, and etc..

If developer choose to define his/her own User-Password based authenticator, it is required that user
    modify configuration option:

	:::bash
	## Replace "io.gearpump.security.CustomAuthenticator" with your real authenticator class.
	gearpump.ui-security.authenticator = "io.gearpump.security.CustomAuthenticator"
		

Make sure CustomAuthenticator extends interface:

	:::scala
	trait Authenticator {
	
	  def authenticate(user: String, password: String, ec: ExecutionContext): Future[AuthenticationResult]
	}


## OAuth2 based authentication

OAuth2 based authentication is commonly use to achieve social login with social network account.

Gearpump provides generic OAuth2 Authentication support which allow user to extend to support new authentication sources.

Basically, OAuth2 based Authentication contains these steps:
 1. User accesses Gearpump UI website, and choose to login with OAuth2 server.
 2. Gearpump UI website redirects user to OAuth2 server domain authorization endpoint.
 3. End user complete the authorization in the domain of OAuth2 server.
 4. OAuth2 server redirects user back to Gearpump UI server.
 5. Gearpump UI server verify the tokens and extract credentials from query
 parameters and form fields.

### Terminologies

For terms like client Id, and client secret, please refers to guide [RFC 6749](https://tools.ietf.org/html/rfc6749)

### Enable web proxy for UI server

To enable OAuth2 authentication, the Gearpump UI server should have network access to OAuth2 server, as
 some requests are initiated directly inside Gearpump UI server. So, if you are behind a firewall, make
 sure you have configured the proxy properly for UI server.

#### If you are on Windows

	:::bash
	set JAVA_OPTS=-Dhttp.proxyHost=xx.com -Dhttp.proxyPort=8088 -Dhttps.proxyHost=xx.com -Dhttps.proxyPort=8088
	bin/services


#### If you are on Linux

	:::bash
    export JAVA_OPTS="-Dhttp.proxyHost=xx.com -Dhttp.proxyPort=8088 -Dhttps.proxyHost=xx.com -Dhttps.proxyPort=8088"
    bin/services


### Google Plus OAuth2 Authenticator

Google Plus OAuth2 Authenticator does authentication with Google OAuth2 service. It extracts the email address
from Google user profile as credentials.

To use Google OAuth2 Authenticator, there are several steps:

1. Register your application (Gearpump UI server here) as an application to Google developer console.
2. Configure the Google OAuth2 information in gear.conf
3. Configure network proxy for Gearpump UI server if applies.

#### Step1: Register your website as an OAuth2 Application on Google

1. Create an application representing your website at [https://console.developers.google.com](https://console.developers.google.com)
2. In "API Manager" of your created application, enable API "Google+ API"
3. Create OAuth client ID for this application. In "Credentials" tab of "API Manager",
choose "Create credentials", and then select OAuth client ID. Follow the wizard
to set callback URL, and generate client ID, and client Secret.

**NOTE:** Callback URL is NOT optional.

#### Step2: Configure the OAuth2 information in gear.conf

1. Enable OAuth2 authentication by setting `gearpump.ui-security.oauth2-authenticator-enabled`
as true.
2. Configure section `gearpump.ui-security.oauth2-authenticators.google` in gear.conf. Please make sure
class name, client ID, client Secret, and callback URL are set properly.

**NOTE:** Callback URL set here should match what is configured on Google in step1.

#### Step3: Configure the network proxy if applies.

To enable OAuth2 authentication, the Gearpump UI server should have network access to Google service, as
 some requests are initiated directly inside Gearpump UI server. So, if you are behind a firewall, make
 sure you have configured the proxy properly for UI server.

For guide of how to configure web proxy for UI server, please refer to section "Enable web proxy for UI server" above.

#### Step4: Restart the UI server and try to click the Google login icon on UI server.

### CloudFoundry UAA server OAuth2 Authenticator

CloudFoundryUaaAuthenticator does authentication by using CloudFoundry UAA OAuth2 service. It extracts the email address
 from Google user profile as credentials.

For what is UAA (User Account and Authentication Service), please see guide: [UAA](https://github.com/cloudfoundry/uaa)

To use Google OAuth2 Authenticator, there are several steps:

1. Register your application (Gearpump UI server here) as an application to UAA with helper tool `uaac`.
2. Configure the Google OAuth2 information in gear.conf
3. Configure network proxy for Gearpump UI server if applies.

#### Step1: Register your application to UAA with `uaac`

1. Check tutorial on uaac at [https://docs.cloudfoundry.org/adminguide/uaa-user-management.html](https://docs.cloudfoundry.org/adminguide/uaa-user-management.html)
2. Open a bash shell, set the UAA server by command `uaac target`

	    :::bash
	    uaac target [your uaa server url]
    
    
3. Login in as user admin by

	    :::bash
	    uaac token client get admin -s MyAdminPassword
	    
    
4. Create a new Application (Client) in UAA,
    
	    :::bash
	    uaac client add [your_client_id]
	      --scope "openid cloud_controller.read"
	      --authorized_grant_types "authorization_code client_credentials refresh_token"
	      --authorities "openid cloud_controller.read"
	      --redirect_uri [your_redirect_url]
	      --autoapprove true
	      --secret [your_client_secret]
	    

#### Step2: Configure the OAuth2 information in gear.conf

1. Enable OAuth2 authentication by setting `gearpump.ui-security.oauth2-authenticator-enabled` as true.
2. Navigate to section `gearpump.ui-security.oauth2-authenticators.cloudfoundryuaa`
3. Config gear.conf `gearpump.ui-security.oauth2-authenticators.cloudfoundryuaa` section.
Please make sure class name, client ID, client Secret, and callback URL are set properly.

**NOTE:** The callback URL here should match what you set on CloudFoundry UAA in step1.

#### Step3: Configure network proxy for Gearpump UI server if applies

To enable OAuth2 authentication, the Gearpump UI server should have network access to Google service, as
 some requests are initiated directly inside Gearpump UI server. So, if you are behind a firewall, make
 sure you have configured the proxy properly for UI server.

For guide of how to configure web proxy for UI server, please refer to please refer to section "Enable web proxy for UI server" above.

#### Step4: Restart the UI server and try to click the CloudFoundry login icon on UI server.

#### Step5: You can also enable additional authenticator for CloudFoundry UAA by setting config:

	:::bash
	additional-authenticator-enabled = true
	

Please see description in gear.conf for more information.

#### Extends OAuth2Authenticator to support new Authorization service like Facebook, or Twitter.

You can follow the Google OAuth2 example code to define a custom OAuth2Authenticator. Basically, the steps includes:

1. Define an OAuth2Authenticator implementation.

2. Add an configuration entry under `gearpump.ui-security.oauth2-authenticators`. For example:

		## name of this authenticator     
    	"socialnetworkx" {
      	  "class" = "io.gearpump.services.security.oauth2.impl.SocialNetworkXAuthenticator"

      	  ## Please make sure this URL matches the name       
      	  "callback" = "http://127.0.0.1:8090/login/oauth2/socialnetworkx/callback"

      	  "clientId" = "gearpump_test2"
      	  "clientSecret" = "gearpump_test2"
      	  "defaultUserRole" = "guest"

      	  ## Make sure socialnetworkx.png exists under dashboard/icons                 
         "icon" = "/icons/socialnetworkx.png"
    	}           
    	
    
   The configuration entry is supposed to be used by class `SocialNetworkXAuthenticator`.