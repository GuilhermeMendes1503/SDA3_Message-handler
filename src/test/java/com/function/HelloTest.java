package com.function;

import com.function.hello.Hello;
import com.function.model.Greeting;
import com.function.model.User;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class HelloTest {

    @Test
    public void test() {
        Greeting result = new Hello().apply(new User("foo"));
        assertThat(result.getMessage()).isEqualTo("Hello, foo!\n");
    }
}