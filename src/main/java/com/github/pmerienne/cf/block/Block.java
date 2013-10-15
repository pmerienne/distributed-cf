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

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

public class Block implements Serializable {

	private static final long serialVersionUID = 3640893414246976396L;

	public long p;
	public long q;
	public int iteration;

	public Block() {
	}

	public Block(long p, long q) {
		this.p = p;
		this.q = q;
		this.iteration = 0;
	}

	public Block(long p, long q, int iteration) {
		this.p = p;
		this.q = q;
		this.iteration = iteration;
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder().append(this.p).append(this.q).toHashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Block))
			return false;
		Block other = (Block) obj;

		return new EqualsBuilder().append(this.p, other.p).append(this.q, other.q).isEquals();
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append(this.q).append(this.p).append(this.iteration).toString();
	}

}
