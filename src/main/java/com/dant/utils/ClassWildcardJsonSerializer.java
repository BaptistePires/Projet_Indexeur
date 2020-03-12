package com.dant.utils;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

public class ClassWildcardJsonSerializer implements JsonSerializer<Class<?>> {
    @Override
    public JsonElement serialize(Class<?> aClass, Type type, JsonSerializationContext jsonSerializationContext) {
        if (aClass == null) return JsonNull.INSTANCE;
        return jsonSerializationContext.serialize(aClass.getTypeName(), String.class);


    }
}
