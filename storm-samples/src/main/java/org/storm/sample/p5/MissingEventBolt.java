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

package org.storm.sample.p5;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.storm.sample.PatternUtils;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;



/**
* Created on 6/27/15.
*/
public class MissingEventBolt extends BaseBasicBolt {
    TimeWindow timeWindow = new TimeWindow(3600);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        long ts = PatternUtils.getInt(tuple,0); 
        
        List<Tuple> collectedTuples = timeWindow.getExpiredEvents(ts);
        for(Tuple t: collectedTuples){
        	collector.emit(new Values(t.getString(2)));
        }
        
        String type = tuple.getString(1).trim();
        if("res".equals(type)){
        	timeWindow.remove(2, tuple.getString(2).trim());
        }else{
        	timeWindow.add(tuple, ts);	
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("MissedRequest"));
    }
    
    
    public static class TimeWindow implements Serializable{
    	List<Tuple> window = new ArrayList<Tuple>();
    	long windowSize; 
    	public TimeWindow(long windowSize){
    		this.windowSize = windowSize;
    	}
    	
    	public void add(Tuple t, long time){
    		window.add(t);
    	}
    	
    	public void remove(int tupleIndex, String value){
    		Iterator<Tuple> it = window.iterator(); 
    		while(it.hasNext()){
    			Tuple t = it.next();
    			if(value.equals(t.getString(tupleIndex).trim())){
    				it.remove();
    			}
    		}
    	}
    	
    	public List<Tuple> getExpiredEvents(long time){
        	List<Tuple> expiredEvents = new ArrayList<Tuple>();
        	Iterator<Tuple> it = window.iterator(); 
    		while(it.hasNext()){
    			Tuple t = it.next();
    			if(time - PatternUtils.getInt(t, 0) > windowSize){
    				expiredEvents.add(t);
    				it.remove();
    			}
    		}
    		return expiredEvents;
    	}
    }
}
