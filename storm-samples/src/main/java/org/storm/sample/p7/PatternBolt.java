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

package org.storm.sample.p7;

import java.util.HashMap;
import java.util.Map;

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
public class PatternBolt extends BaseBasicBolt {
    Map<String, Boolean> smallTxMap = new HashMap<String, Boolean>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        long amount = PatternUtils.getInt(tuple,2); 
        if(amount < 10){
        	smallTxMap.put(PatternUtils.getString(tuple, 1), true);
        }else if (amount > 1000){
        	if(smallTxMap.remove(PatternUtils.getString(tuple, 1)) != null){
        		collector.emit(new Values(PatternUtils.getString(tuple, 0), PatternUtils.getString(tuple, 1)));
        	}
        	smallTxMap.put(PatternUtils.getString(tuple, 1), true);
        }        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp", "customerID"));
    }
        
}
