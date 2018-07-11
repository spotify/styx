/*
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2018 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.styx.util;

import io.opencensus.trace.Annotation;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.EndSpanOptions;
import io.opencensus.trace.Link;
import io.opencensus.trace.Span;
import io.opencensus.trace.SpanContext;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Span has final methods and cannot be spied/mocked
public class MockSpan extends Span {

  public boolean ended;
  public final Map<String, AttributeValue> attributes = new HashMap<>();
  public io.opencensus.trace.Status status;
  public final List<Annotation> annotations = new ArrayList<>();

  public MockSpan() {
    super(SpanContext.INVALID, EnumSet.noneOf(Options.class));
  }

  @Override
  public void putAttribute(String key, AttributeValue value) {
    attributes.put(key, value);
  }

  @Override
  public void addAnnotation(String description, Map<String, AttributeValue> attributes) {
    addAnnotation(Annotation.fromDescriptionAndAttributes(description, attributes));
  }

  @Override
  public void addAnnotation(Annotation annotation) {
    annotations.add(annotation);
  }

  @Override
  public void addLink(Link link) {
    // nop
  }

  @Override
  public void setStatus(io.opencensus.trace.Status status) {
    this.status = status;
  }

  @Override
  public void end(EndSpanOptions options) {
    this.ended = true;
  }
}
