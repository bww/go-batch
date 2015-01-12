// 
// Go Batch
// Copyright (c) 2015 Brian W. Wolter, All rights reserved.
// 
// Redistribution and use in source and binary forms, with or without modification,
// are permitted provided that the following conditions are met:
// 
//   * Redistributions of source code must retain the above copyright notice, this
//     list of conditions and the following disclaimer.
// 
//   * Redistributions in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.
//     
//   * Neither the names of Brian W. Wolter nor the names of the contributors may
//     be used to endorse or promote products derived from this software without
//     specific prior written permission.
//     
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
// IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
// INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
// OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
// OF THE POSSIBILITY OF SUCH DAMAGE.
// 

/*
  Simple batching abstraction.
*/
package batch

import (
  "time"
)

/**
 * Culling type
 */
type Culling int

/**
 * Consolidation cosntants
 */
const (
  CULL_NONE   Culling = iota
  CULL_FIRST
  CULL_LAST
)

/**
 * A batcher
 */
type Batcher struct {
  input     chan interface{}
  output    chan []interface{}
  timeout   time.Duration
  culling   Culling
  size      uint
}

/**
 * A batcher
 */
func NewBatcher(s uint, c Culling, t time.Duration) *Batcher {
  b := &Batcher{}
  b.input = make(chan interface{}, s)
  b.output = make(chan []interface{})
  b.timeout = t
  b.culling = c
  b.size = s
  go b.proc()
  return b
}

/**
 * Process input
 */
func (b *Batcher) proc() {
  for {
    count := int(b.size)
    batch := make([]interface{}, 0, count)
    timeout := time.After(b.timeout)
    
    // get at least one result
    e, ok := <- b.input
    if !ok { return }
    batch = batch[:1]
    batch[0] = e
    
    // add as many other as are available up to the batch size before our flush timeout
    outer:
    for i := len(batch); i < count; i++ {
      select {
        
        case e, ok = <- b.input:
          if !ok {
            return
          }else{
            batch = batch[:i+1]
            batch[i] = e
          }
          
        case <- timeout:
          break outer
          
      }
    }
    
    // add to output
    b.output <- batch
    
  }
}

/**
 * Add an element
 */
func (b *Batcher) Add(e interface{}) {
  b.input <- e
}

/**
 * Obtain the output channel
 */
func (b *Batcher) Batch() <-chan []interface{} {
  return b.output
}

