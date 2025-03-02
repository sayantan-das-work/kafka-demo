package com.demo.kafka.producer;

public class ProduceException extends Exception{
    private String exMsg;
    private String exThrownFor;
    public ProduceException(String exMessage, Object exRecord) {
        this.exMsg = exMessage;
        this.exThrownFor = exMessage;
    }

    @Override
    public String getMessage() {
        return String.format("Exception thrown for [ %s ] \n Exception :: %s\n",exThrownFor, exMsg);
    }
}
