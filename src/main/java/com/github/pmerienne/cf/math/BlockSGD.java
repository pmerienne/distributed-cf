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
package com.github.pmerienne.cf.math;

import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.lang.math.RandomUtils;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.github.pmerienne.cf.block.MatrixBlock;
import com.github.pmerienne.cf.rating.Rating;
import com.github.pmerienne.cf.util.MathUtil;

public class BlockSGD extends BaseFunction {

	private static final long serialVersionUID = -2075072514219299644L;

	/**
	 * Step size
	 */
	private final double initialStepSize;

	/**
	 * Penalty parameter
	 */
	private final double lambda;

	/**
	 * Features count
	 */
	private final int k;
	
	private final Prediction prediction = new Prediction();

	public BlockSGD(double stepSize, double lambda, int k) {
		super();
		this.initialStepSize = stepSize;
		this.lambda = lambda;
		this.k = k;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		try {
			MatrixBlock up = (MatrixBlock) tuple.get(0);
			if (up == null) {
				up = new MatrixBlock();
			}

			MatrixBlock vq = (MatrixBlock) tuple.get(1);
			if (vq == null) {
				vq = new MatrixBlock();
			}

			Collection<Rating> ratings = (Collection<Rating>) tuple.get(2);
			if (ratings == null) {
				ratings = new HashSet<>();
			}
			
			int iteration = tuple.getInteger(3);

			this.process(up, vq, ratings, iteration);
			collector.emit(new Values(up, vq));
		} catch (NullPointerException npe) {
			// missing parameters
		}

	}

	/**
	 * <pre>
	 * 
	 * ui ← ui + η ⋅ (eui ⋅ vj - λ ⋅ ui)
	 * vj ← vj + η ⋅ (eui ⋅ ui - λ ⋅ vj)
	 * 
	 * bu ← bu + η ⋅ (eui - λ ⋅ bu)
	 * bi ← bi + η ⋅ (eui - λ ⋅ bi)
	 * </pre>
	 * 
	 * @param up
	 * @param vq
	 * @param ratings
	 * @param iteration 
	 */
	public void process(MatrixBlock up, MatrixBlock vq, Collection<Rating> ratings, int iteration) {
		// TODO : shuffle ratings
		
		double stepSize = 2 * initialStepSize / (iteration + 1);
		for (Rating rating : ratings) {
			long i = rating.getI();
			long j = rating.getJ();
			double value = rating.getValue();

			double[] ui = up.getFeatures(i);
			if (ui == null) {
				ui = this.randomVector();
			}

			double[] vj = vq.getFeatures(j);
			if (vj == null) {
				vj = this.randomVector();
			}

			double userBias = up.getBias(i);
			double itemBias = vq.getBias(j);
			
			// eui = rui - uiT vj
			double prediction = this.prediction.predict(ui, vj, userBias, itemBias);
			double eui = value - prediction;

			// ui ← ui + η ⋅ (eui ⋅ vj - λ ⋅ ui )
			double[] newUi = MathUtil.add(ui, MathUtil.mult(MathUtil.subtract(MathUtil.mult(vj, eui), MathUtil.mult(ui, lambda)), stepSize));
			// vj ← vj + η ⋅ (eui ⋅ ui - λ ⋅ vj )
			double[] newVj = MathUtil.add(vj, MathUtil.mult(MathUtil.subtract(MathUtil.mult(ui, eui), MathUtil.mult(vj, lambda)), stepSize));
			
			 // bu ← bu + η ⋅ (eui - λ ⋅ bu)
			userBias = userBias + stepSize * (eui - lambda * userBias); 
			
			// bi ← bi + η ⋅ (eui - λ ⋅ bi)
			itemBias = itemBias + stepSize * (eui - lambda * itemBias); 

			up.setFeatures(i, newUi);
			up.setBias(i, userBias);
			vq.setFeatures(j, newVj);
			vq.setBias(j, itemBias);
			
		}
	}

	protected double[] randomVector() {
		double[] vector = new double[this.k];
		for (int i = 0; i < this.k; i++) {
			vector[i] = RandomUtils.nextDouble() * 1 / this.k;
		}
		return vector;
	}

}
