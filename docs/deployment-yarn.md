---
layout: global
title: Deployment with YARN
---

## How to Start the Gearpump cluster on YARN
1. Create HDFS folder /user/gearpump/, make sure all read-write rights are granted.
2. Either build gearpump or have access to the distribution file.
3. Upload the distribution gearpump-pack-2.11.5-{{ site.GEARPUMP_VERSION }}.tar.gz jars to HDFS folder: /user/gearpump/, you can refer to [How to get gearpump distribution](get-gearpump-distribution.html) to get the Gearpump binary.
4. Modify the config file ```conf/yarn.conf``` to either enable or disable services (gearpump.services.enabled).
5. Start the gearpump yarn cluster, for example 
``` bash
bin/yarnclient -version gearpump-pack-2.11.5-{{ site.GEARPUMP_VERSION }} -config conf/yarn.conf
```
