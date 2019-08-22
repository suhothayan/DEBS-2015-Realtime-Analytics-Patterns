/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
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

package org.storm.sample.p3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.storm.sample.PatternUtils;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


/**
* Batch windows
*/
public class WindowBolt extends BaseBasicBolt {
    TimeWindow timeWindow = new TimeWindow(60000);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String tsAsStr = tuple.getString(3);
        long ts = Long.parseLong(tsAsStr.trim());
        
        if(timeWindow.hasExpired(ts)){
        	List<Tuple> collectedTuples = timeWindow.reset(ts);
        	long sum = 0; 
        	for(Tuple t: collectedTuples){
                long price = PatternUtils.getInt(t, 1);
                long amount = PatternUtils.getInt(t, 2);
                sum = sum + (price * amount);
        	}
        	collector.emit(new Values(sum));
        }
        
        timeWindow.add(tuple, ts);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("60Secvalue"));
    }
    
    
    public static class TimeWindow implements Serializable{
    	List<Tuple> window = new ArrayList<Tuple>();
    	long startTime = -1;
    	long windowSize; 
    	public TimeWindow(long windowSize){
    		this.windowSize = windowSize;
    	}
    	
    	public void add(Tuple t, long time){
    		window.add(t);
    	}
    	
    	public boolean hasExpired(long time){
    		if(startTime == -1){
    			startTime = time;
    		}
    		return time - startTime > windowSize;
    	}
    	
    	public List<Tuple> reset(long time){
    		startTime = time;
    		List<Tuple> copy = window; 
    		window = new ArrayList<Tuple>();
    		return copy;
    	}
    }
}
