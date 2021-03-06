/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.pgasync.impl;


import com.github.pgasync.Connection;
import rx.Observable;

/**
 * @author Antti Laisi
 */
public class ConnectionValidator {

    final String validationQuery;

    public ConnectionValidator(String validationQuery) {
        this.validationQuery = validationQuery;
    }

    Observable<Connection> validate(Connection connection) {
        if(validationQuery == null) {
            return Observable.just(connection);
        }
        return Observable.create(subscriber -> connection.queryRows(validationQuery)
                                                    .subscribe(row -> { }, subscriber::onError,
                                                            () -> {
                                                                subscriber.onNext(connection);
                                                                subscriber.onCompleted();
                                                            }));
    }
}
