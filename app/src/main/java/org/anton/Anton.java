package org.anton;

public class Anton {
    public String getGreeting() {
        return "Hello World!";
    }

    public static void main(String[] args) {
        System.out.println(new Anton().getGreeting());
    }
}