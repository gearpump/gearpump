Grafana is a visualation frontend for graphite. Here is the guide to enable grafana dashboard:

1. Find a machine to host your grafana and graphite service.
2. install docker on this machine.
3. docker pull kamon/grafana_graphite
4. docker run -d -v /etc/localtime:/etc/localtime:ro -p 801:80 -p 8125:8125/udp -p 8126:8126 -p2003:2003 --name kamon-grafana-dashboard kamon/grafana_graphite
5. Configure the ip, and graphite port in conf/application.conf 
6. Browser http://ip:801/, you should see a grafana dashboard.
7. Import file graphana_dashboard to grafana.


The UI looks like this:

![](https://raw.githubusercontent.com/clockfly/gearpump/master/doc/dashboard.png)
