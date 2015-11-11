---
layout: global
title: Deployment with YARN
---

## How to Start the Gearpump cluster on YARN
1. Create HDFS folder /user/gearpump/, make sure all read-write rights are granted.
2. Upload the gearpump-{{ site.GEARPUMP_VERSION }}.tar.gz jars to HDFS folder: /user/gearpump/, you can refer to [How to get gearpump distribution](get-gearpump-distribution.html) to get the Gearpump binary.
3. Modify the config file ```conf/yarn.conf.template``` or create your own config file
4. Start the gearpump yarn cluster, for example 
``` bash
bin/yarnclient -version gearpump-{{ site.GEARPUMP_VERSION }} -config conf/yarn.conf
```