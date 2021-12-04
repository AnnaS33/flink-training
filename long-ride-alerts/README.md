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

### Упражнение 4: LongRidesExercise.
Цель этого упражнения состоит в том, чтобы выдавать ID для поездок на такси продолжительностью более двух часов. Вы должны предположить, что события поездки на такси могут быть потеряны.
В конечном итоге вы должны очистить любое созданное вами состояние.

За нас уже было написано получение потока из источника. Были установлены водяные и временные метки, а так же проведена группировка по ключу - rideId.

Как известно для каждой поездки существует два собития - начало и конец. Так как мы сгруппировали всё по Id поездок, то теперь каждое значение состояния будет содержать оба этих события для каждой поездки, либо одно из них, если имеет место потеря данных или событие ещё не завершилось. Поэтому проверяем все возможные случаи.
Знакомимся с методом registerEventTimeTimer, который регистрирует таймер, который будет запущен, когда водяной знак времени события пройдет заданное время, и deleteEventTimeTimer, который помогает удалить таймер.

![](4_ex_cod1.png)

![](4_ex_cod2.png)

![](4_ex_cod3.png)

Тест

![](4_ex_test.png)
















