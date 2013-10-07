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

import static org.fest.assertions.Assertions.assertThat;

import org.fest.assertions.Delta;
import org.junit.Test;

import com.github.pmerienne.cf.block.MatrixBlock;

public class MatrixBlockTest {

	@Test
	public void should_get_and_set_values() {
		// Given
		MatrixBlock matrixBlock = new MatrixBlock();
		double[] vector0 = new double[] { 0.1, 1.2 };
		double[] vector1 = new double[] { 3.4, 5.6 };

		// When
		matrixBlock.setFeatures(0, vector0);
		matrixBlock.setFeatures(1, vector1);

		// Then
		assertThat(matrixBlock.getFeatures(0)).isEqualTo(vector0, Delta.delta(10e-3));
		assertThat(matrixBlock.getFeatures(1)).isEqualTo(vector1, Delta.delta(10e-3));
		assertThat(matrixBlock.getFeatures(2)).isNull();
	}

	@Test
	public void should_get_null_value_if_no_value() {
		// Given
		MatrixBlock matrixBlock = new MatrixBlock();

		// Then
		assertThat(matrixBlock.getFeatures(0)).isNull();
	}

	@Test
	public void should_find_existing_value() {
		// Given
		MatrixBlock matrixBlock = new MatrixBlock();
		double[] vector0 = new double[] { 0.1, 1.2 };

		// When
		matrixBlock.setFeatures(0, vector0);

		// Then
		assertThat(matrixBlock.featuresExists(0)).isTrue();
	}
}
