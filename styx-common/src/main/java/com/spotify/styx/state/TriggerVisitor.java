/*- 
 * -\-\- 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 * -/-/- 
 */

package com.spotify.styx.state;

import com.github.sviperll.adt4j.GenerateValueClassForVisitor;
import com.github.sviperll.adt4j.Getter;
import com.github.sviperll.adt4j.Visitor;

/**
 * Generated {@link Trigger} ADT for all the triggers injected in the system
 */
@GenerateValueClassForVisitor(isPublic = true)
@Visitor(resultVariableName = "R")
public interface TriggerVisitor<R> {

  R natural(@Getter String triggerId);
  R adhoc(@Getter String triggerId);
  R backfill(@Getter String triggerId);
  R unknown(@Getter String triggerId);
}
