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

### Упражнение 3: HourlyTipsExercise.

Задача упражнения состоит в том, чтобы сначала подсчитать общее количество чаевых, собранных каждым водителем, в час, а затем из этого потока найдите самую высокую общую сумму чаевых за каждый час.

В данной работе необходимо будет работать со временем, а значит понадобиться использовать водные и временные метки. В качестве этих меток устанавливаем время начала поезки.
Затем группируем по ID водителя, затем разбиваем по временным окнам продолжительностью час и рассчитываем сумму полученных чаевых для каждого окна.
Среди них ищем максимум, затем записываем полученный результат в приёмник.

![](https://github.com/AnnaS33/flink-training/blob/master/3_ex_cod1.png)

![](https://github.com/AnnaS33/flink-training/blob/master/3_ex_cod2.png)

Тест

![](https://github.com/AnnaS33/flink-training/blob/master/3_ex_test.png)















