package io.latent.storm.rabbitmq;

import io.latent.storm.rabbitmq.config.ConnectionConfig;
import io.latent.storm.rabbitmq.config.ProducerConfig;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pns.alltypes.rabbitmq.config.RabbitConnectionConfig;
import pns.alltypes.rabbitmq.io.AmqpChannel;
import pns.alltypes.rabbitmq.sustained.RabbitMQConnectionManager;
import backtype.storm.topology.ReportedFailedException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;

public class RabbitMQProducer implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQProducer.class);
    private final Declarator declarator;

    private static RabbitMQConnectionManager RABBIT_MQ_CONNECTION_MANAGER;
    private transient Logger logger;

    private transient ConnectionConfig connectionConfig;
    private transient AmqpChannel channel;

    private static final Lock rmqInitLock = new ReentrantLock();
    private static transient volatile boolean opened = false;

    public RabbitMQProducer() {
        this(new Declarator.NoOp());
    }

    public RabbitMQProducer(final Declarator declarator) {
        this.declarator = declarator;
    }

    private void createRabbitChannel() {
        RabbitMQProducer.LOGGER.info("Trying to reget a channel");
        this.channel = RabbitMQProducer.RABBIT_MQ_CONNECTION_MANAGER.closeAndReopenChannel(this.channel);
    }

    public void send(final Message message) {
        if (message == Message.NONE) {
            return;
        }
        sendMessageActual((Message.MessageForSending) message);
    }

    private void sendMessageActual(final Message.MessageForSending message) {

        // wait until channel is avaialable
        boolean regetChannel = false;

        try {
            final AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder().contentType(message.getContentType())
                    .contentEncoding(message.getContentEncoding()).deliveryMode(message.isPersistent() ? 2 : 1).headers(message.getHeaders()).build();
            channel.getChannel().basicPublish(message.getExchangeName(), message.getRoutingKey(), properties, message.getBody());
            if (RabbitMQProducer.LOGGER.isTraceEnabled()) {
                RabbitMQProducer.LOGGER.trace("Succesfully published rabbitmq message");
            }
        } catch (final AlreadyClosedException ace) {
            regetChannel = true;
            logger.error("already closed exception while attempting to send message", ace);
            throw new ReportedFailedException(ace);
        } catch (final IOException ioe) {
            regetChannel = true;
            logger.error("io exception while attempting to send message", ioe);
            throw new ReportedFailedException(ioe);
        } catch (final Exception e) {
            logger.warn("Unexpected error while sending message. Backing off for a bit before trying again (to allow time for recovery)", e);
            try {
                Thread.sleep(1000);
            } catch (final InterruptedException ie) {
            }
        } finally {

            if (regetChannel) {
                createRabbitChannel();
            }

        }
    }

    public void open(final Map config) {
        logger = LoggerFactory.getLogger(RabbitMQProducer.class);
        connectionConfig = ProducerConfig.getFromStormConfig(config).getConnectionConfig();
        internalOpen();
    }

    private void internalOpen() {
        try {
            RabbitMQProducer.rmqInitLock.lock();
            if (!RabbitMQProducer.opened) {
                RabbitMQProducer.RABBIT_MQ_CONNECTION_MANAGER = RabbitMQConnectionManager.getInstance(new RabbitConnectionConfig(connectionConfig
                        .asConnectionFactory(), connectionConfig.getHighAvailabilityHosts() == null ? null : connectionConfig.getHighAvailabilityHosts()
                        .toAddresses()));
                RabbitMQProducer.opened = true;
                RabbitMQProducer.LOGGER.trace(String.format("Using RABBIT_MQ_CONNECTION_MANAGER %s", RabbitMQProducer.RABBIT_MQ_CONNECTION_MANAGER));
            }

            RabbitMQProducer.RABBIT_MQ_CONNECTION_MANAGER.hintResourceAddition();
            createRabbitChannel();

            // run any declaration prior to message sending
            declarator.execute(channel.getChannel());
        } catch (final Exception e) {
            logger.error("could not open connection on rabbitmq", e);

        } finally {
            RabbitMQProducer.rmqInitLock.unlock();
        }
    }

    public void close() {
        // shutdown of rmq done on exit of VM.
    }

}
