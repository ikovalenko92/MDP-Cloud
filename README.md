# MDP - Cloud Manufacturing - Cloud Team

Install/setup the programs in this order:
Kafka->Hadoop->InfluxDB->Grafana

For an overview of this project works please visit this presentation and watch the accompanying video:
https://prezi.com/4zdlawrgpltz/data-pipelining-and-real-time-data-monitoring/?utm_campaign=share&utm_medium=copy

Other information:
Questions and requests should go to hpc-support@umich.edu

Kafka Server can only be accessed from the flux-hadoop cluster or from the Windows computer connected with the testbed machine
Zookeeper and Kafka host is now
migsae-kafka.aura.arc-ts.umich.edu
zookeeper port=2181
kafka port=9092

INfluxDB program & Grafana must be setup on a computer that is on the Umich network or can VPN with the network otherwise it will not be able to access it through the firewall

InfluxDB accounts:
read/write access account
username: cloud_data
pass: 2016SummerProj

Read access account
username: graphana_acc
pass: testBed_read16

InfluxDB
https://migsae-influx.arc-ts.umich.edu:8086

Grafana:
https://migsea-influx.arc-ts.umich.edu


