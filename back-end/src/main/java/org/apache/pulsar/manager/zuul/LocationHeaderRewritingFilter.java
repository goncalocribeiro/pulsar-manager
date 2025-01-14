/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pulsar.manager.zuul;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UrlPathHelper;
import reactor.core.publisher.Mono;

import java.net.URI;

/**
 * Handle http redirection.
 */
@Component
public class LocationHeaderRewritingFilter implements GatewayFilter {

    private final UrlPathHelper urlPathHelper = new UrlPathHelper();

    @Value("${redirect.host}")
    private String host;

    @Value("${redirect.port}")
    private String port;

    @Value("${redirect.scheme}")
    private String scheme;

    private final RouteLocator routeLocator;

    @Autowired
    public LocationHeaderRewritingFilter(RouteLocator routeLocator) {
        this.routeLocator = routeLocator;
    }

    private static final String LOCATION_HEADER = "Location";

    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        // Proceed only if the response status is 3xx redirection
        return chain.filter(exchange).then(Mono.defer(() -> {
            HttpStatusCode status = exchange.getResponse().getStatusCode();
            if (status != null && status.is3xxRedirection()) {
                String location = exchange.getResponse().getHeaders().getFirst(LOCATION_HEADER);
                if (location != null) {
                    return rewriteLocation(location, exchange);
                }
            }
            return Mono.empty();
        }));
    }

    private Mono<Void> rewriteLocation(String location, ServerWebExchange exchange) {
        // Get the matching route for the current path
        return routeLocator.getRoutes()
                .flatMap(route -> {
                    // Apply the AsyncPredicate asynchronously and get the result
                    return Mono.from(route.getPredicate().apply(exchange))
                            .filter(Boolean::booleanValue) // Keep only true results
                            .flatMap(predicateMatch -> {
                                // Modify location if route matches
                                String modifiedLocation = modifyLocation(location, exchange, route);
                                exchange.getResponse().getHeaders().set(LOCATION_HEADER, modifiedLocation);
                                return Mono.empty();
                            });
                })
                .defaultIfEmpty(Mono.fromRunnable(() -> {
                    // Fallback: if no matching route, leave the location unchanged
                    exchange.getResponse().getHeaders().set(LOCATION_HEADER, location);
                }))
                .then(); // Indicate the end of the operation (returns Mono<Void>)
    }

    private String modifyLocation(String location, ServerWebExchange exchange, Route route) {
        UriComponents redirectedUriComps = UriComponentsBuilder.fromUriString(location).build();

        // Build the modified URI with the desired host, scheme, and port
        UriComponentsBuilder redirectedUriBuilder = UriComponentsBuilder
                .fromUriString(location)
                .scheme(scheme)
                .host(host)
                .port(port)
                .replacePath(redirectedUriComps.getPath())
                .queryParam("redirect", true)
                .queryParam("redirect.scheme", redirectedUriComps.getScheme())
                .queryParam("redirect.host", redirectedUriComps.getHost())
                .queryParam("redirect.port", redirectedUriComps.getPort());

        return redirectedUriBuilder.toUriString();
    }
}
