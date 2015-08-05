package io.latent.storm.rabbitmq;

import io.latent.storm.rabbitmq.config.ConnectionConfig;

import java.io.Serializable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pns.alltypes.rabbitmq.config.RabbitConnectionConfig;
import pns.alltypes.rabbitmq.io.AmqpChannel;
import pns.alltypes.rabbitmq.sustained.RabbitMQConnectionManager;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * An abstraction on RabbitMQ client API to encapsulate interaction with RabbitMQ and de-couple Storm API from RabbitMQ
 * API.
 * @author peter@latent.io
 */
public class RabbitMQConsumer implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQConsumer.class);
    public static final long MS_WAIT_FOR_MESSAGE = 1L;

    private final Address[] highAvailabilityHosts;
    private final int prefetchCount;
    private final String queueName;
    private final boolean requeueOnFail;
    private final Declarator declarator;
    private final ErrorReporter reporter;
    private final Logger logger;

    private AmqpChannel channel;
    private QueueingConsumer consumer;
    private String consumerTag;

    private static final Lock rmqInitLock = new ReentrantLock();
    private static transient volatile boolean opened = false;

    private static RabbitMQConnectionManager RABBIT_MQ_CONNECTION_MANAGER;

    public RabbitMQConsumer(final ConnectionConfig connectionConfig, final int prefetchCount, final String queueName, final boolean requeueOnFail,
            final Declarator declarator, final ErrorReporter errorReporter) {
        try {
            RabbitMQConsumer.rmqInitLock.lock();

            if (!RabbitMQConsumer.opened) {
                RabbitMQConsumer.RABBIT_MQ_CONNECTION_MANAGER = RabbitMQConnectionManager.getInstance(new RabbitConnectionConfig(connectionConfig
                        .asConnectionFactory(), connectionConfig.getHighAvailabilityHosts() == null ? null : connectionConfig.getHighAvailabilityHosts()
                                .toAddresses()));
                RabbitMQConsumer.opened = true;
                RabbitMQConsumer.LOGGER.trace(String.format("Using RABBIT_MQ_CONNECTION_MANAGER %s,", RabbitMQConsumer.RABBIT_MQ_CONNECTION_MANAGER));

            }

        } finally {
            RabbitMQConsumer.rmqInitLock.unlock();
        }
        RabbitMQConsumer.RABBIT_MQ_CONNECTION_MANAGER.hintResourceAddition();
        this.highAvailabilityHosts = connectionConfig.getHighAvailabilityHosts().toAddresses();
        this.prefetchCount = prefetchCount;
        this.queueName = queueName;
        this.requeueOnFail = requeueOnFail;
        this.declarator = declarator;

        this.reporter = errorReporter;
        this.logger = LoggerFactory.getLogger(RabbitMQConsumer.class);
    }

    public Message nextMessage() {
        if (consumerTag == null || consumer == null) {
            return Message.NONE;
        }
        boolean regetChannel = false;
        try {
            return Message.forDelivery(consumer.nextDelivery(RabbitMQConsumer.MS_WAIT_FOR_MESSAGE));
        } catch (final ShutdownSignalException sse) {
            regetChannel = true;
            logger.error("shutdown signal received while attempting to get next message", sse);
            reporter.reportError(sse);
            return Message.NONE;
        } catch (final InterruptedException ie) {
            /* nothing to do. timed out waiting for message */
            logger.debug("interruepted while waiting for message", ie);
            return Message.NONE;
        } catch (final ConsumerCancelledException cce) {
            regetChannel = true;
            /* if the queue on the broker was deleted or node in the cluster containing the queue failed */
            logger.error("consumer got cancelled while attempting to get next message", cce);
            reporter.reportError(cce);
            return Message.NONE;
        } finally {
            if (regetChannel) {
                createRabbitChannel();
                createConsumer();
            }
        }
    }

    private void createRabbitChannel() {
        logger.info("Trying to reget a channel");
        this.channel = RabbitMQConsumer.RABBIT_MQ_CONNECTION_MANAGER.closeAndReopenChannel(this.channel);
    }

    public void ack(final Long msgId) {
        boolean regetChannel = false;

        try {
            channel.getChannel().basicAck(msgId, false);
        } catch (final ShutdownSignalException sse) {
            regetChannel = true;
            logger.error("shutdown signal received while attempting to ack message", sse);
            reporter.reportError(sse);
        } catch (final Exception e) {
            logger.error("could not ack for msgId: " + msgId, e);
            reporter.reportError(e);
        } finally {
            if (regetChannel) {
                createRabbitChannel();
            }
        }
    }

    public void fail(final Long msgId) {
        if (requeueOnFail) {
            failWithRedelivery(msgId);
        } else {
            deadLetter(msgId);
        }
    }

    public void failWithRedelivery(final Long msgId) {
        boolean regetChannel = false;

        try {
            channel.getChannel().basicReject(msgId, true);
        } catch (final ShutdownSignalException sse) {
            regetChannel = true;
            logger.error("shutdown signal received while attempting to fail with redelivery", sse);
            reporter.reportError(sse);
        } catch (final Exception e) {
            logger.error("could not fail with redelivery for msgId: " + msgId, e);
            reporter.reportError(e);
        } finally {
            if (regetChannel) {
                createRabbitChannel();
            }
        }
    }

    public void deadLetter(final Long msgId) {
        boolean regetChannel = false;

        try {
            channel.getChannel().basicReject(msgId, false);
        } catch (final ShutdownSignalException sse) {
            regetChannel = true;
            logger.error("shutdown signal received while attempting to fail with no redelivery", sse);
            reporter.reportError(sse);
        } catch (final Exception e) {
            logger.error("could not fail with dead-lettering (when configured) for msgId: " + msgId, e);
            reporter.reportError(e);
        } finally {
            if (regetChannel) {
                createRabbitChannel();
            }
        }
    }

    public void open() {

        try {

            createRabbitChannel();
            if (prefetchCount > 0) {
                logger.info("setting basic.qos / prefetch count to " + prefetchCount + " for " + queueName);
                channel.getChannel().basicQos(prefetchCount);
            }
            // run any declaration prior to queue consumption
            declarator.execute(channel.getChannel());

            createConsumer();
        } catch (final Exception e) {
            logger.error("could not open listener on queue " + queueName);
            reporter.reportError(e);
        } finally {

        }
    }

    private void createConsumer() {
        try {
            final Channel ioChannel = channel.getChannel();
            consumer = new QueueingConsumer(ioChannel);
            consumerTag = ioChannel.basicConsume(queueName, isAutoAcking(), consumer);
        } catch (final Exception e) {
            logger.error("could not open listener on queue " + queueName);
            reporter.reportError(e);
        } finally {

        }

    }

    protected boolean isAutoAcking() {
        return false;
    }

    public void close() {

    }

}
