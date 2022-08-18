package org.apache.calcite.adapter.druid.query;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.calcite.adapter.druid.DruidJson;

import java.io.IOException;
import java.io.StringWriter;

public abstract class BaseDruidQuery implements DruidJson {

  public String toQuery() {
    final StringWriter sw = new StringWriter();
    try {
      final JsonFactory factory = new JsonFactory();
      final JsonGenerator generator = factory.createGenerator(sw);

      this.write(generator);

      generator.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return sw.toString();
  }
}


