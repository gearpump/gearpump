---
layout: global
title: UI Dashboard Authentication and Authorization
---

## How to enable UI authentication?

1. Change config file gear.conf, find entry `gearpump-ui.gearpump.ui-security.authentication-enabled`, change the value to true

   ```
   gearpump-ui.gearpump.ui-security.authentication-enabled = true
   ```
   
   Restart the UI dashboard, and then the UI authentication is enabled. It will prompt for user name and password.
   
## How to add or remove user?

For the default authentication plugin, it has three categories of users: admins, users, and guests.


* admins: have unlimited permission, like shutdown a cluster, add/remove machines.
* users: have limited permission to submit an application and etc..
* guests: can not submit/kill applications, but can view the application status.

System administrator can add or remove user by updating config file `conf/gear.conf`.  

Suppose we want to add user jerry as an administrator, here are the steps:
  
1. Pick a password, and generate the digest for this password. Suppose we use password `ilovegearpump`, 
   to generate the digest:
   
   ```
   bin/gear io.gearpump.security.PasswordUtil -password  ilovegearpump
   ```
   
   It will generate a digest value like this:
   
   ```
   CgGxGOxlU8ggNdOXejCeLxy+isrCv0TrS37HwA==
   ```

2. Change config file conf/gear.conf at path `gearpump-ui.gearpump.ui-security.config-file-based-authenticator.admins`,
   add user `jerry` in this list:
   
   ```
   admins = {
      ## Default Admin. Username: admin, password: admin
      ## !!! Please replace this builtin account for production cluster for security reason. !!!
      "admin" = "AeGxGOxlU8QENdOXejCeLxy+isrCv0TrS37HwA=="
      "jerry" = "CgGxGOxlU8ggNdOXejCeLxy+isrCv0TrS37HwA=="
   }
   ```

3. Restart the UI dashboard by `bin/services` to make the change effective.

4. Group "admins" have very unlimited permission, you may want to restrict the permission. In that case 
   you can modify `gearpump-ui.gearpump.ui-security.config-file-based-authenticator.users` or
   `gearpump-ui.gearpump.ui-security.config-file-based-authenticator.guests`.

5. See description at `conf/gear.conf` to find more information.   
   
## What is the default user and password?

Gearpump distribution is shipped with two default users:

1. username: admin, password: admin
2. username: guest, password: guest

User `admin:admin` has unlimited permissions, while guest can only view the application status. Guest account cannot 
submit or kill the application by UI console.

For security reason, you need to remove the default user "admin" and "guest" for production cluster.

## Is this secure?

Firstly, we will NOT store any user password in any way so only the user himself knows the password. 
We will use one-way sha1 digest to verify the user input password. As it is a one-way hashing,
so generally it is safe.

1. Digest flow(from original password to digest):
 
   ```
   random salt byte array of length 8 -> byte array of (salt + sha1(salt, password)) -> base64Encode
   ```
   
2. Verification user input password with stored digest:
 
   ```
   base64Decode -> extract salt -> do sha1(salt, password) -> generate digest: salt + sha1 ->
   compare the generated digest with the stored digest.
   ```
