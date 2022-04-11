package com.maersk.producer.consumer.library.aspect;

import com.maersk.producer.consumer.library.kafka.KafkaProducer;
import com.maersk.producer.consumer.library.services.MessagePublisherService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

@Slf4j
@Aspect
@Component
public class ProducerAspect <T> {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private KafkaProducer <T> producer;

    @Autowired
    private MessagePublisherService<T> messagePublisherService;

    private static final String NOTIFICATION_TOPIC = "${kafka.notification.topic}";
    private static final String KAFKA_HEADERS = "${kafka.headers.keys}";
    private static final String KAFKA_HEADERS_COUNT = "${kafka.headers.count}";
    private static final String KAFKA_EVENT_TOPIC = "${kafka.notification.%s.topic}";
    private static final String KAFKA_EVENT_HEADERS = "${kafka.notification.%s.headers.keys}";
    private static final String KAFKA_EVENT_HEADERS_COUNT = "${kafka.notification.%s.headers.count}";


    @Pointcut("@annotation(com.maersk.producer.consumer.library.annotations.Produce)")
    public void produce(){
    }

    @Pointcut("@annotation(com.maersk.producer.consumer.library.annotations.EventProducer)")
    public void eventProducer(){
    }

    @Around(value = "produce()")
    public void publishMessageAdvice(ProceedingJoinPoint joinPoint)
    {
        log.info("Inside publishMessageAdvice");
        try
        {
            var args = joinPoint.getArgs();
            joinPoint.proceed();
            if (Objects.nonNull(args[0]))
            {
                var headerCount = context.getEnvironment().resolvePlaceholders(KAFKA_HEADERS_COUNT);
                var producerTopic = context.getEnvironment().resolvePlaceholders(NOTIFICATION_TOPIC);
                ProducerRecord<String, T> producerRecord = new ProducerRecord<>(producerTopic, (T) args[0]);
                var headCount = getValidHeaderCount(headerCount);
                if (headCount > 0)
                {
                    addKafkaHeaders(producerRecord.headers(), headCount, args);
                }
                messagePublisherService.publishOnTopic(producerRecord);
            }

        } catch (Exception e)
        {
            log.error("Exception in intercepted method ", e);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    @Around(value = "eventProducer()")
    public void produceEventAdvice(ProceedingJoinPoint joinPoint)
    {
        log.info("Inside produceEventAdvice");
        try
        {
            var args = joinPoint.getArgs();
            joinPoint.proceed();
            if (Objects.nonNull(args[0]) && Objects.nonNull(args[1]))
            {
                var eventName = args[0].toString();
                var topicPlaceholder = String.format(KAFKA_EVENT_TOPIC, eventName);
                var producerTopic = context.getEnvironment().resolvePlaceholders(topicPlaceholder);
                ProducerRecord<String, T> producerRecord = new ProducerRecord<>(producerTopic, (T) args[1]);
                buildKafkaHeaders(eventName, producerRecord.headers(), args);
                messagePublisherService.publishOnTopic(producerRecord);
            }

        } catch (Exception e)
        {
            log.error("Exception in intercepted method ", e);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    private void addKafkaHeaders(Headers headers, int headerCount, Object[] args)
    {
        var headersFromAppContext = Arrays.asList(context.getEnvironment().resolvePlaceholders(KAFKA_HEADERS).split(","));
        log.info("headersFromAppContext: {}", headersFromAppContext);
        int count = 0;
        for (String header: headersFromAppContext) {
           count++;
           headers.add(header, args[count].toString().getBytes(StandardCharsets.UTF_8));
        }
        log.info("headers after 1st loop: {}", headers);

        for (int i =1; i <headerCount+1; i++)
        {
            headers.add(headersFromAppContext.get(i-1), args[i].toString().getBytes(StandardCharsets.UTF_8));
        }
        log.info("headers after 2nd loop: {}", headers);
    }

    private void buildKafkaHeaders(String event, Headers headers, Object[] args)
    {
        var headersFromAppContext = Arrays.asList(context.getEnvironment()
                .resolvePlaceholders(String.format(KAFKA_EVENT_HEADERS, event)).split(","));
        int count = 1;
        for (String header: headersFromAppContext) {
            count++;
            headers.add(header.trim(), args[count].toString().getBytes(StandardCharsets.UTF_8));
        }
        log.info("headers after loop: {}", headers);
    }

    private int getValidHeaderCount(String headerCount)
    {
        if (!headerCount.startsWith("${"))
        {
            return Integer.parseInt(headerCount);
        }
        return 0;
    }
}
