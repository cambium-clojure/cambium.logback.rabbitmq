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

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.spi.AppenderAttachable;
import ch.qos.logback.core.spi.AppenderAttachableImpl;

public abstract class FailoverEnabledAppender
extends UnsynchronizedAppenderBase<ILoggingEvent> implements AppenderAttachable<ILoggingEvent> {

    protected abstract void errorProneAppend(final ILoggingEvent event) throws Exception;

    protected final AppenderAttachableImpl<ILoggingEvent> aai = new AppenderAttachableImpl<ILoggingEvent>();

    private int appenderCount = 0;

    @Override
    protected final void append(final ILoggingEvent event) {
        try {
            errorProneAppend(event);
        } catch (final Exception e) {
            appendFailed(event, e);
        }
    }

    protected void appendFailed(ILoggingEvent event, Exception e) {
        // execute fall back appender
        aai.appendLoopOnAppenders(event);
    }

    public void addAppender(Appender<ILoggingEvent> newAppender) {
        if (appenderCount == 0) {
            appenderCount++;
            addInfo("Attaching appender named [" + newAppender.getName() + "] to FailoverEnabledAppender.");
            aai.addAppender(newAppender);
        } else {
            addWarn("One and only one appender may be attached to FailoverEnabledAppender.");
            addWarn("Ignoring additional appender named [" + newAppender.getName() + "]");
        }
    }

    // ----- getters & setters -----

    public int getAppenderCount() {
        return appenderCount;
    }

    public void setAppenderCount(int count) {
        this.appenderCount = count;
    }

}
