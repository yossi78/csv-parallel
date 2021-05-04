package com.veeva.csvparallel.api;


import lombok.AllArgsConstructor;
import lombok.ToString;

@AllArgsConstructor
public class Human {

    public String name;
    public  int age;

    @Override
    public String toString() {
        return name + " , " +  age;
    }
}
