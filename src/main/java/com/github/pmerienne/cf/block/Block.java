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
package com.github.pmerienne.cf.block;

import java.io.Serializable;

public class Block implements Serializable {

	private static final long serialVersionUID = 3640893414246976396L;

	private long p;
	private long q;

	public Block() {
	}

	public Block(long p, long q) {
		super();
		this.p = p;
		this.q = q;
	}

	public long getP() {
		return p;
	}

	public void setP(long p) {
		this.p = p;
	}

	public long getQ() {
		return q;
	}

	public void setQ(long q) {
		this.q = q;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (p ^ (p >>> 32));
		result = prime * result + (int) (q ^ (q >>> 32));
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
		Block other = (Block) obj;
		if (p != other.p)
			return false;
		if (q != other.q)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Block [p=" + p + ", q=" + q + "]";
	}

}
