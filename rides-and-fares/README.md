<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

### Упражнение 2: RidesAndFaresExercise.

Цель этого упражнения - обогатить поездки на такси информацией о тарифах.
Для регения задачи мы переопределяем методы класса:

open - в нем мы получаем состояния для TaxiRide и TaxiFare.

![](https://github.com/AnnaS33/flink-training/blob/master/2_ex_cod1.png)

FlatMap1 и FlatMap2 - вызываются для элементов первого и второго потока соответственно.
Когда мы работаем со значением из состояния, то насамом деле мы работаем с набором значений, которые сгруппированы по ключу, в данном случае по ID поездки.

![](https://github.com/AnnaS33/flink-training/blob/master/2_ex_cod2.png)

Тест

![](https://github.com/AnnaS33/flink-training/blob/master/2_ex_test.png)














