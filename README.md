# storm-rabbitmq

[![Build Status](https://travis-ci.org/ppat/storm-rabbitmq.png)](https://travis-ci.org/ppat/storm-rabbitmq)
This is a fork of original storm-rabbitmq plugin by ppat but addresses the issue of Rabbit connection management transparently.

## RabbitMQ connection management
This fork aims at acheiving a pseudo Highly Available RabbitMQ by having the connections persistent.  Each consumer is automatically assigned a connection by the pool and the connections are maintained without the knowledge of the plugin doing anything about it. 

This is especially handy when the Rabbit queue connection fails intermediately.

This plugin was written because of the fact that the storm topology was going down when there is a shutdown signal exception. This is recovered currently by maintaining a persistent connection to the Rabbit and recreating silently in the background.

The Rabbit connections are made persistent and hence for a large real time process computing with varying load pattern will not cause more retries. 

Note:The connections needs to have heartbeat configured so that the connection is made persistent
