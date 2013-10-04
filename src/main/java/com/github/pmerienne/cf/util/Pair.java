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
