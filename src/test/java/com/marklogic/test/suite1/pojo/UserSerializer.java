package com.marklogic.test.suite1.pojo;

import java.io.IOException;
import java.util.Set;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public class UserSerializer extends StdSerializer<User> {
    
    public UserSerializer() {
        this(null);
    }
   
    public UserSerializer(Class<User> t) {
        super(t);
    }
 
    @Override
    public void serialize(
      User value, JsonGenerator jgen, SerializerProvider provider) 
      throws IOException, JsonProcessingException {
  
        jgen.writeStartObject();
        jgen.writeStringField("id", value.guid);
        jgen.writeEndObject();
        jgen.writeEmbeddedObject(value.getTags());
        
        
    }
}