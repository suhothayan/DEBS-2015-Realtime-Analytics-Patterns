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

package org.storm.sample.p4;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


/**
* Created on 6/27/15.
*/
public class JoinBolt extends BaseBasicBolt {
    Tuple ballLocation; 
    Map<String, Tuple> map = new HashMap<String, Tuple>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String sensorID = tuple.getString(1);
        if("b".equals(sensorID.trim())){
        	ballLocation = tuple;
        	for(Tuple t: map.values()){
        		if(hasHitHappend(ballLocation, t)){
        			collector.emit(new Values(t.getString(1)));	
        		}
        	}
        }else{
    		if(hasHitHappend(ballLocation, tuple)){
    			collector.emit(new Values(tuple.getString(1)));	
    		}
    		map.put(tuple.getString(1), tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("PalyerWhoHit"));
    }
    
    
    public long getInt(Tuple t, int index){
    	return Integer.parseInt(t.getString(index).trim());
    }
    
    
    public boolean hasHitHappend(Tuple bt, Tuple pt){
		if(getInt(bt, 5) > 55 && 
				getInt(bt, 0) - getInt(pt, 0) < 1000){
			long dx = Math.abs(getInt(bt, 2) - getInt(pt, 2));
			long dy = Math.abs(getInt(bt, 3) - getInt(pt, 3));
			long dz = Math.abs(getInt(bt, 4) - getInt(pt, 4));
			double dall = Math.sqrt(Math.pow(dx, 2) + Math.pow(dy, 2) + Math.pow(dz, 2));
			if(dall < 1000){
				return true;
			}
		}
		return false;
    }
}
