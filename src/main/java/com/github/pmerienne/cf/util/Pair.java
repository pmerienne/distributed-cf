/**
 * Copyright 2013-2015 Pierre Merienne
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.pmerienne.cf.util;

import java.io.Serializable;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

public class Pair<V1, V2> implements Serializable {

	private static final long serialVersionUID = 255898201499304800L;

	private V1 first;
	private V2 second;

	public Pair() {
	}

	public Pair(V1 first, V2 second) {
		this.first = first;
		this.second = second;
	}

	public V1 getFirst() {
		return first;
	}

	public void setFirst(V1 first) {
		this.first = first;
	}

	public V2 getSecond() {
		return second;
	}

	public void setSecond(V2 second) {
		this.second = second;
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder().append(this.first).append(this.second).toHashCode();
	}

	@Override
	@SuppressWarnings("unchecked")
	public boolean equals(Object obj) {
		if (!(obj instanceof Pair))
			return false;

		Pair<V1, V2> other = (Pair<V1, V2>) obj;
		return new EqualsBuilder().append(this.first, other.first).append(this.second, other.second).isEquals();
	}

	@Override
	public String toString() {
		return "Pair [first=" + first + ", second=" + second + "]";
	}

}
