package org.apache.calcite.adapter.druid;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

public class DruidJsonTopNMetric implements DruidJson {
  private String dimension;
  private String dimensionOrder;
  private String direction;
  private boolean isGroupByKey;

  public DruidJsonTopNMetric(String dimension, boolean isGroupByKey,
      String dimensionOrder, String direction) {
    this.dimension = dimension;
    this.dimensionOrder = dimensionOrder;
    this.direction = direction;
    this.isGroupByKey = isGroupByKey;
  }

  @Override
  public void write(JsonGenerator generator) throws IOException {
    boolean isDESC = direction.equals("descending");
    if (!isGroupByKey && isDESC) {
      generator.writeString(dimension);
      return;
    }

    boolean needInverted = (isGroupByKey == isDESC);
    if (needInverted) {
      generator.writeStartObject();
      generator.writeStringField("type", "inverted");
    }

    if (!isGroupByKey) {
      generator.writeStringField("metric", dimension);
    } else if (!needInverted) {
      DruidQuery.writeObject(generator, new DruidJsonDimensionMetric(dimensionOrder));
    } else {
      DruidQuery.writeField(generator, "metric", new DruidJsonDimensionMetric(dimensionOrder));
    }

    if (needInverted) {
      generator.writeEndObject();
    }
  }


  protected static class DruidJsonDimensionMetric implements DruidJson {

    private String dimensionOrder;

    public DruidJsonDimensionMetric(String dimensionOrder) {
      this.dimensionOrder = dimensionOrder;
    }

    @Override
    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", "dimension");
      DruidQuery.writeField(generator, "ordering", new DruidJsonDimensionOrder(dimensionOrder));
      generator.writeEndObject();
    }
  }

  protected static class DruidJsonDimensionOrder implements DruidJson {
    private String dimensionOrder;

    public DruidJsonDimensionOrder(String dimensionOrder) {
      this.dimensionOrder = dimensionOrder;
    }

    @Override
    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", dimensionOrder);
      generator.writeEndObject();
    }
  }

}
