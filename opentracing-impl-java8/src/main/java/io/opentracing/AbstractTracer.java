/**
 * Copyright 2016 The OpenTracing Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.opentracing;

import io.opentracing.propagation.Extractor;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.Injector;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

abstract class AbstractTracer implements Tracer {

    private final PropagationRegistry registry = new PropagationRegistry();

    protected AbstractTracer() {}

    abstract AbstractSpanBuilder createSpanBuilder(String operationName);

    @Override
    public SpanBuilder buildSpan(String operationName){
        return createSpanBuilder(operationName);
    }

    @Override
    public <C> void inject(SpanContext spanContext, Format<? super C> format, C carrier) {
        @SuppressWarnings("unchecked")
        Class<C> carrierType = (Class<C>) carrier.getClass();  // self-type
        registry.getInjector(carrierType).inject(spanContext, carrier);
    }

    @Override
    public <C> SpanContext extract(Format<? super C> format, C carrier) {
        @SuppressWarnings("unchecked")
        Class<C> carrierType = (Class<C>) carrier.getClass();  // self-type
        return registry.getExtractor(carrierType).extract(carrier);
    }

    public <C> Injector<C> register(Class<C> carrierType, Injector<C> injector) {
        return registry.register(carrierType, injector);
    }

    public <C> Extractor<C> register(Class<C> carrierType, Extractor<C> extractor) {
        return registry.register(carrierType, extractor);
    }

    private static class PropagationRegistry {
        // For any class<C> key, the corresponding injector/extractor has type Injector<C>.
        private final ConcurrentMap<Class<?>, Injector<?>> injectors = new ConcurrentHashMap<>();
        private final ConcurrentMap<Class<?>, Extractor<?>> extractors = new ConcurrentHashMap<>();

        public <C> Injector<? super C> getInjector(Class<C> carrierType) {
            Class<?> c = carrierType;
            // match first on concrete classes
            do {
                if (injectors.containsKey(c)) {
                    // Safe since c can only be a super-type of carrierType.
                    @SuppressWarnings("unchecked")
                    Injector<? super C> injector = (Injector<? super C>) injectors.get(c);
                    return injector;
                }
                c = c.getSuperclass();
            } while (c != null);
            // match second on interfaces
            for (Class<?> iface : carrierType.getInterfaces()) {
                if (injectors.containsKey(iface)) {
                    // Safe since iface can only be a super-type of carrierType.
                    @SuppressWarnings("unchecked")
                    Injector<? super C> injector = (Injector<? super C>) injectors.get(iface);
                    return injector;
                }
            }
            throw new AssertionError("no registered injector for " + carrierType.getName());
        }

        public <C> Extractor<? super C> getExtractor(Class<C> carrierType) {
            Class<?> c = carrierType;
            // match first on concrete classes
            do {
                if (extractors.containsKey(c)) {
                    // Safe since c can only be a super-type of carrierType.
                    @SuppressWarnings("unchecked")
                    Extractor<? super C> extractor = (Extractor<? super C>) extractors.get(c);
                    return extractor;
                }
                c = c.getSuperclass();
            } while (c != null);
            // match second on interfaces
            for (Class<?> iface : carrierType.getInterfaces()) {
                if (extractors.containsKey(iface)) {
                    // Safe since iface can only be a super-type of carrierType.
                    @SuppressWarnings("unchecked")
                    Extractor<? super C> extractor = (Extractor<? super C>) extractors.get(iface);
                    return extractor;
                }
            }
            throw new AssertionError("no registered extractor for " + carrierType.getName());
        }

        public <C> Injector<C> register(Class<C> carrierType, Injector<C> injector) {
            // Safe since the only way to add new injectors is above.
            @SuppressWarnings("unchecked")
            Injector<C> result = (Injector<C>) injectors.putIfAbsent(carrierType, injector);
            return result;
        }

        public <C> Extractor<C> register(Class<C> carrierType, Extractor<C> extractor) {
            // Safe since the only way to add new extractors is above.
            @SuppressWarnings("unchecked")
            Extractor<C> result = (Extractor<C>) extractors.putIfAbsent(carrierType, extractor);
            return result;
        }
    }

}
