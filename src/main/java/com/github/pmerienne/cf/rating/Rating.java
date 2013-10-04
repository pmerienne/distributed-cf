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
package com.github.pmerienne.cf.rating;

import java.io.Serializable;

public class Rating implements Serializable {

	private static final long serialVersionUID = -5863388596658497805L;

	public long i;
	public long j;
	public double value;

	public Rating() {
	}

	public Rating(long i, long j, double value) {
		super();
		this.i = i;
		this.j = j;
		this.value = value;
	}

	public long getI() {
		return i;
	}

	public void setI(long i) {
		this.i = i;
	}

	public long getJ() {
		return j;
	}

	public void setJ(long j) {
		this.j = j;
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (i ^ (i >>> 32));
		result = prime * result + (int) (j ^ (j >>> 32));
		long temp;
		temp = Double.doubleToLongBits(value);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Rating other = (Rating) obj;
		if (i != other.i)
			return false;
		if (j != other.j)
			return false;
		if (Double.doubleToLongBits(value) != Double.doubleToLongBits(other.value))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Rating [i=" + i + ", j=" + j + ", value=" + value + "]";
	}

}
