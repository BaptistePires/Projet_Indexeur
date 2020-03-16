package com.dant.utils;

import com.dant.entity.Column;
import com.dant.exception.UnsupportedTypeException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class Test {

    public static void main(String[] args) throws ClassNotFoundException, UnsupportedTypeException {
        Column c = new Column("test", "Integer");
        Map<Class<?>, List<?>> map = new HashMap<>();


        Function<String, ?> caster = Integer::parseInt;
        System.out.println((caster.getClass()).toString());
        Object o = caster.apply("16");
        System.out.println(o);
    }

    public static <T> T cast(Class<T> cls, Object o) {
        return cls.cast(o);
    }
}
