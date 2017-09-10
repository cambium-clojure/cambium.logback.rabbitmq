/**
 *   Copyright (c) Shantanu Kumar. All rights reserved.
 *   The use and distribution terms for this software are covered by the
 *   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
 *   which can be found in the file LICENSE at the root of this distribution.
 *   By using this software in any fashion, you are agreeing to be bound by
 *   the terms of this license.
 *   You must not remove this notice, or any other, from this software.
 **/

package cambium.logback.rabbitmq;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Layout;

/**
 * A Logback appender that publishes log data to a rabbitmq exchange.
 */
public class RabbitAppender extends FailoverEnabledAppender {

    /**
     * The Layout used to format the log message body.
     */
    protected Layout<ILoggingEvent> layout;

    /**
     * AMQP server connection parameters
     */
    protected String routingKey;
    protected String exchange;

    protected String host;
    protected Integer port;
    protected String virtualHost;
    protected String username;
    protected String password;
    protected Integer requestedHeartbeat;


    /**
     * Connection and Channel properties that will be
     * initialized using the above provided properties.
     */
    private volatile boolean needChannel = true;
    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Channel channel;

    @Override
    public void start() {

        // Create a Connection and Channel to the AMQP server
        // using supplied properties.
        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(this.host);
        connectionFactory.setUsername(this.username);
        connectionFactory.setPassword(this.password);
        connectionFactory.setVirtualHost(this.virtualHost);
        connectionFactory.setConnectionTimeout(1000);
        if (port != null) {
            connectionFactory.setPort(this.port);
        }
        if (requestedHeartbeat != null) {
            connectionFactory.setRequestedHeartbeat(requestedHeartbeat);
        }
        try {
            connect();
        } catch (final IOException e) {
            addError("Unable to connect to RabbitMQ", e);
        } catch (final TimeoutException e) {
            addError("Timeout when connecting to RabbitMQ", e);
        }
        super.start();
    }

    @Override
    public synchronized void stop() {
        disconnect();
        super.stop();
    }

    protected void maybeConnect() throws IOException, TimeoutException {
        if (isStarted() &&
                (this.needChannel || this.connection == null || !this.connection.isOpen() || this.channel == null)) {
            connect();
        }
    }

    protected void disconnect() {
        try {
            if (this.channel != null) {
                try {
                    this.channel.close();
                } catch (Exception ex) {
                    // swallowed
                } finally {
                    this.channel = null;
                }
            }

            if (this.connection != null) {
                try {
                    this.connection.close();
                } catch (Exception ex) {
                    // swallowed
                } finally {
                    this.connection = null;
                }
            }
        } finally {
            this.needChannel = true;
        }
    }

    protected synchronized void connect() throws IOException, TimeoutException {
        if (this.needChannel || this.connection == null || !this.connection.isOpen() || this.channel == null) {
            disconnect();
            this.connection = this.connectionFactory.newConnection();
            this.channel = this.connection.createChannel();
            this.needChannel = false;
        }
    }

    @Override
    protected void errorProneAppend(final ILoggingEvent event) throws IOException, TimeoutException {
        // Use the provided layout to format the logging event
        // and set that as the AMQP message payload.
        final String message = getLayout().doLayout(event);
        final byte[] payload = message.getBytes(StandardCharsets.UTF_8);

        maybeConnect();
        channel.basicPublish(
                getExchange(),
                getRoutingKey(),
                new AMQP.BasicProperties.Builder()
                        .headers(Collections.singletonMap(
                                "message-distribution-hash", (Object) ThreadLocalRandom.current().nextInt()))
                        .build(),
                payload);
    }

    @Override
    protected void appendFailed(ILoggingEvent event, Exception e) {
        // report the status (possibly to the console)
        addError("Could not publish log message to rabbitmq", e);

        // disconnect (to trigger reconnect)
        disconnect();

        super.appendFailed(event, e);
    }

    // ----- getters & setters -----

    public Layout<ILoggingEvent> getLayout() {
        return layout;
    }

    public void setLayout(Layout<ILoggingEvent> layout) {
        this.layout = layout;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getVirtualHost() {
        return virtualHost;
    }

    public void setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Integer getRequestedHeartbeat() {
        return requestedHeartbeat;
    }

    public void setRequestedHeartbeat(Integer requestedHeartbeat) {
        this.requestedHeartbeat = requestedHeartbeat;
    }

    public boolean isNeedChannel() {
        return needChannel;
    }

    public void setNeedChannel(boolean needChannel) {
        this.needChannel = needChannel;
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    // ----- delegated methods -----

    @Override
    public boolean detachAppender(Appender<ILoggingEvent> paramAppender) {
        return aai.detachAppender(paramAppender);
    }

    @Override
    public boolean detachAppender(String paramString) {
        return aai.detachAppender(paramString);
    }

    @Override
    public void detachAndStopAllAppenders() {
        aai.detachAndStopAllAppenders();
    }

    @Override
    public Appender<ILoggingEvent> getAppender(String paramString) {
        return aai.getAppender(paramString);
    }

    @Override
    public boolean isAttached(Appender<ILoggingEvent> paramAppender) {
        return aai.isAttached(paramAppender);
    }

    @Override
    public Iterator<Appender<ILoggingEvent>> iteratorForAppenders() {
        return aai.iteratorForAppenders();
    }

}
