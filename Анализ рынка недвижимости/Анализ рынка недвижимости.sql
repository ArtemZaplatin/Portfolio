/* Проект первого модуля: анализ данных для агентства недвижимости
 * Часть 2. Решаем ad hoc задачи
 * 
 * Автор: Заплатин Артем Александрович
 * Дата: 30.01.2025
*/

-- Пример фильтрации данных от аномальных значений
-- Определим аномальные значения (выбросы) по значению перцентилей:
WITH limits AS (
    SELECT
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY total_area) AS total_area_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS rooms_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony) AS balcony_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_h,
        PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_l
    FROM real_estate.flats
),
-- Найдем id объявлений, которые не содержат выбросы:
filtered_id AS(
    SELECT id
    FROM real_estate.flats  
    WHERE 
        total_area < (SELECT total_area_limit FROM limits)
        AND (rooms < (SELECT rooms_limit FROM limits) OR rooms IS NULL)
        AND (balcony < (SELECT balcony_limit FROM limits) OR balcony IS NULL)
        AND ((ceiling_height < (SELECT ceiling_height_limit_h FROM limits)
            AND ceiling_height > (SELECT ceiling_height_limit_l FROM limits)) OR ceiling_height IS NULL)
    )
-- Выведем объявления без выбросов:
SELECT *
FROM real_estate.flats
WHERE id IN (SELECT * FROM filtered_id);


-- Задача 1: Время активности объявлений
-- Результат запроса должен ответить на такие вопросы:
-- 1. Какие сегменты рынка недвижимости Санкт-Петербурга и городов Ленинградской области 
--    имеют наиболее короткие или длинные сроки активности объявлений?
-- 2. Какие характеристики недвижимости, включая площадь недвижимости, среднюю стоимость квадратного метра, 
--    количество комнат и балконов и другие параметры, влияют на время активности объявлений? 
--    Как эти зависимости варьируют между регионами?
-- 3. Есть ли различия между недвижимостью Санкт-Петербурга и Ленинградской области по полученным результатам?

WITH limits AS (
    SELECT  
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY total_area) AS total_area_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS rooms_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony) AS balcony_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_h,
        PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_l
    FROM real_estate.flats     
),
-- Найдём id объявлений, которые не содержат выбросы:
filtered_id AS(
    SELECT id
    FROM real_estate.flats  
    WHERE 
        total_area < (SELECT total_area_limit FROM limits)
        AND (rooms < (SELECT rooms_limit FROM limits) OR rooms IS NULL)
        AND (balcony < (SELECT balcony_limit FROM limits) OR balcony IS NULL)
        AND ((ceiling_height < (SELECT ceiling_height_limit_h FROM limits)
            AND ceiling_height > (SELECT ceiling_height_limit_l FROM limits)) OR ceiling_height IS NULL)
    ),
-- Подготовка данных: категоризация по СПб и ЛенОбл, категор. по перодам активности. + считаем стоим. кв.метра, фильтр по типу "город"
data_analysis as (
	select 
		case 
			when city = 'Санкт-Петербург' 
				then  'Санкт-Петербург'
			else 'Ленинградская область'
		end as region_city,
		case 
			when days_exposition <= 30 
				then  'До месяца'
			when days_exposition <= 90
				then 'До квартала'
			when days_exposition <= 180
				then 'До полугода'
			when days_exposition > 180
				then 'Более полугода'
			else 'Объявление активно'
		end as period_exposition,
		round(avg((last_price/total_area)::numeric)) as avg_price_total_area, -- Средняя стоимость кв. метра
		count(id) as cnt_advertisement, -- Кол-во объявлений
		PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_area) as median_total_area, -- Медиана по общей площади
		round(avg(total_area::numeric), 1) as avg_total_area, -- средняя общей площади
		PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY rooms) as median_cnt_room, -- Медиана по кол-ву комнат
		round(avg(rooms::numeric), 1) as avg_rooms, -- средняя по комнатам
		PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY balcony) as median_cnt_balcony, --  Медиана по кол-ву балконов
		round(avg(balcony::numeric), 1) as avg_balcony, -- средняя по балконам
		PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY floor) as median_floor, -- Медиана по этажу
		round(avg(floor)::numeric, 1) as avg_floor, -- средняя по этажу
		round(avg(ceiling_height::numeric), 1) as avg_ceiling_height, -- средняя высота потолка
		round(avg(airports_nearest::numeric), 1) as avg_airports_nearest, -- среднее расстояние до ближ. аэропорта
		PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY parks_around3000) as median_parks_around3000, -- медиана по числу парков в радиусе 3км
		PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ponds_around3000) as median_ponds_around3000, -- медиана по числу водоемов в радиусе 3км
		count(id) filter(where is_apartment = 1) as cnt_apartment, -- кол-во апартаментов
		count(id) filter(where open_plan = 1) as cnt_open_plan -- кол-во с открытой планировкой
	FROM real_estate.advertisement
	left join real_estate.flats using(id)
	left join real_estate.city using(city_id)
	left join real_estate.type using(type_id)
	WHERE id IN (SELECT * FROM filtered_id) and type = 'город' and extract(year from first_day_exposition) != 2014 and extract(year from first_day_exposition) != 2019
	group by region_city, period_exposition)
select *,
	round(cnt_advertisement*100/sum(cnt_advertisement) over(partition by region_city), 1) as perc_adv_region, -- доля по региону
	round(cnt_apartment*100/sum(cnt_advertisement) over(partition by region_city), 2) as perc_apart, -- доля апартаментов
	round(cnt_open_plan*100/sum(cnt_advertisement) over(partition by region_city), 2) as perc_open_plan -- доля с открытой планеровкой
from data_analysis;

-- Задача 2: Сезонность объявлений
-- Результат запроса должен ответить на такие вопросы:
-- 1. В какие месяцы наблюдается наибольшая активность в публикации объявлений о продаже недвижимости? 
--    А в какие — по снятию? Это показывает динамику активности покупателей.
-- 2. Совпадают ли периоды активной публикации объявлений и периоды, 
--    когда происходит повышенная продажа недвижимости (по месяцам снятия объявлений)?
-- 3. Как сезонные колебания влияют на среднюю стоимость квадратного метра и среднюю площадь квартир? 
--    Что можно сказать о зависимости этих параметров от месяца?

WITH limits AS (
    SELECT  
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY total_area) AS total_area_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS rooms_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony) AS balcony_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_h,
        PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_l
    FROM real_estate.flats     
),
-- Найдём id объявлений, которые не содержат выбросы:
filtered_id AS(
    SELECT id
    FROM real_estate.flats  
    WHERE 
        total_area < (SELECT total_area_limit FROM limits)
        AND (rooms < (SELECT rooms_limit FROM limits) OR rooms IS NULL)
        AND (balcony < (SELECT balcony_limit FROM limits) OR balcony IS NULL)
        AND ((ceiling_height < (SELECT ceiling_height_limit_h FROM limits)
            AND ceiling_height > (SELECT ceiling_height_limit_l FROM limits)) OR ceiling_height IS NULL)
    ),
table_first as (  -- таблица с выставленными объявлениями
	select 
		extract(month from first_day_exposition) as month, -- месяц
		count(a.id) as cnt_first_exp, -- кол-во объявлений
		round(avg(last_price/total_area)::numeric, 2) as avg_price_total_area_first_exp, -- Средняя стоимость кв. метра
		round(avg(total_area)::numeric, 2) as avg_total_area_first_exp -- Средняя кол-во кв. метров
	from real_estate.advertisement a
	left join real_estate.flats using(id)
	left join real_estate.type using(type_id)
	WHERE id IN (SELECT * FROM filtered_id) 
		and extract(year from first_day_exposition) != 2014 and extract (year from first_day_exposition) != 2019 
		and type = 'город'
	group by month 
	order by month),
table_last as ( -- таблица с завершенными объявлениями
	select 
		extract(month from first_day_exposition + days_exposition::int) as month, -- месяц
		count(a.id) as cnt_last_exp, -- кол-во завершенных объявлений
		round(avg(last_price/total_area)::numeric, 2) as avg_price_total_area_last_exp, -- Средняя стоимость кв. метра
		round(avg(total_area)::numeric, 2) as avg_total_area_last_exp -- Средняя кол-во кв. метров
	from real_estate.advertisement a
	left join real_estate.flats using(id)
	left join real_estate.type using(type_id)
	WHERE id IN (SELECT * FROM filtered_id) 
		and extract(year from first_day_exposition) != 2014 and extract (year from first_day_exposition) != 2019 
		and (first_day_exposition + days_exposition::int) is not null 
		and type = 'город'
	group by month
	order by month)
select 
	month, 															-- месяц
	cnt_first_exp, 													-- кол-во опубликованных объявлений
	rank() over(order by cnt_first_exp desc) as rank_first_exp, 	-- ранг опубликованных
	cnt_last_exp, 													-- кол-во завершенных объявлений
	rank() over(order by cnt_last_exp desc) as rank_last_exp, 		-- ранг завершенных
	round(cnt_last_exp*100/cnt_first_exp::numeric, 2) as perc_last_exp, -- доля завершенных
	avg_price_total_area_first_exp, 								-- Средняя стоимость кв. метра у опубликованных
	avg_price_total_area_last_exp, 									-- Средняя стоимость кв. метра у завершенных
	avg_total_area_first_exp,										-- Средняя кол-во кв. метров у опубликованных
	avg_total_area_last_exp 										-- Средняя кол-во кв. метров у завершенных
from table_first f
full join table_last l using(month)
order by month;
/*
 * ВТОРОЙ ВАРИАНТ РЕШЕНИЯ С ГОДАМИ 
 * 
 * */
-- Определим аномальные значения (выбросы) по значению перцентилей:
WITH limits AS (
    SELECT  
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY total_area) AS total_area_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS rooms_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony) AS balcony_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_h,
        PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_l
    FROM real_estate.flats     
),
-- Найдём id объявлений, которые не содержат выбросы:
filtered_id AS(
    SELECT id
    FROM real_estate.flats  
    WHERE 
        total_area < (SELECT total_area_limit FROM limits)
        AND (rooms < (SELECT rooms_limit FROM limits) OR rooms IS NULL)
        AND (balcony < (SELECT balcony_limit FROM limits) OR balcony IS NULL)
        AND ((ceiling_height < (SELECT ceiling_height_limit_h FROM limits)
            AND ceiling_height > (SELECT ceiling_height_limit_l FROM limits)) OR ceiling_height IS NULL)
    ),
table_first as (
	select 
		extract(month from first_day_exposition) as month,
		extract(year from first_day_exposition) as year,
		count(a.id) as cnt_first_exp,
		round(avg(last_price/total_area)::numeric, 2) as avg_price_total_area_first_exp, -- Средняя стоимость кв. метра
		round(avg(total_area)::numeric, 2) as avg_total_area_first_exp -- Средняя кол-во кв. метров
	from real_estate.advertisement a
	left join real_estate.flats using(id)
	WHERE id IN (SELECT * FROM filtered_id) and extract(year from first_day_exposition) != 2014 and extract(year from first_day_exposition) != 2019
	group by month, year 
	order by month),
table_last as (
	select 
		extract(month from first_day_exposition + days_exposition::int) as month,
		extract(year from first_day_exposition + days_exposition::int) as year,
		count(a.id) as cnt_last_exp,
		round(avg(last_price/total_area)::numeric, 2) as avg_price_total_area_last_exp, -- Средняя стоимость кв. метра
		round(avg(total_area)::numeric, 2) as avg_total_area_last_exp -- Средняя кол-во кв. метров
	from real_estate.advertisement a
	left join real_estate.flats using(id)
	WHERE id IN (SELECT * FROM filtered_id) 
		and extract(year from first_day_exposition) != 2014
		and extract (year from first_day_exposition) != 2019 
		and (first_day_exposition + days_exposition::int) is not null
	group by month, year
	order by month)
select 
	f.year,
	f.month,
	cnt_first_exp,
	rank() over(order by cnt_first_exp desc) as rank_first_exp,
	cnt_last_exp,
	rank() over(order by cnt_last_exp desc) as rank_first_exp,
	round(cnt_last_exp*100/cnt_first_exp::numeric, 2) as perc_last_exp, -- доля завершенных
	avg_price_total_area_first_exp,
	avg_price_total_area_last_exp,
	avg_total_area_first_exp,
	avg_total_area_last_exp
from table_first f
full join table_last l on f.month = l.month and f.year = l.year
where l.year is not null and f.year is not null
order by year, month;


-- Задача 3: Анализ рынка недвижимости Ленобласти
-- Результат запроса должен ответить на такие вопросы:
-- 1. В каких населённые пунктах Ленинградской области наиболее активно публикуют объявления о продаже недвижимости?
-- 2. В каких населённых пунктах Ленинградской области — самая высокая доля снятых с публикации объявлений? 
--    Это может указывать на высокую долю продажи недвижимости.
-- 3. Какова средняя стоимость одного квадратного метра и средняя площадь продаваемых квартир в различных населённых пунктах? 
--    Есть ли вариация значений по этим метрикам?
-- 4. Среди выделенных населённых пунктов какие пункты выделяются по продолжительности публикации объявлений? 
--    То есть где недвижимость продаётся быстрее, а где — медленнее.

WITH limits AS (
    SELECT  
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY total_area) AS total_area_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS rooms_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony) AS balcony_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_h,
        PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_l
    FROM real_estate.flats     
),
-- Найдём id объявлений, которые не содержат выбросы:
filtered_id AS(
    SELECT id
    FROM real_estate.flats  
    WHERE 
        total_area < (SELECT total_area_limit FROM limits)
        AND (rooms < (SELECT rooms_limit FROM limits) OR rooms IS NULL)
        AND (balcony < (SELECT balcony_limit FROM limits) OR balcony IS NULL)
        AND ((ceiling_height < (SELECT ceiling_height_limit_h FROM limits)
            AND ceiling_height > (SELECT ceiling_height_limit_l FROM limits)) OR ceiling_height IS NULL)
    ),
-- Подготовка данных: категоризация по СПб и ЛенОбл, категор. по перодам активности. + считаем стоим. кв.метра, фильтр по типу "город"
len_obl_filter as (
	select 
		count(id) as cnt_id
	FROM real_estate.advertisement
	full join real_estate.flats using(id)
	full join real_estate.city using(city_id)
	WHERE id IN (SELECT * FROM filtered_id) and city != 'Санкт-Петербург'),
data_analysis as (
	select
		NTILE(4) over(order by avg(days_exposition)) as rank,
		city,
		-- Кол-во объявлений и доля от всех объявлений
		count(id) as cnt_advertisement, 
		round(count(id)::numeric/(select cnt_id	FROM len_obl_filter)*100, 2) as perc_cnt_adv,
		-- кол-во завершенных объявлений и доля завершенных объявлений от всех н/п
		count(days_exposition) as cnt_exp, 
		round(count(days_exposition)*100/count(id)::numeric, 2) as perc_cnt_exp,
		-- Медиана по длительности продажи и средняя по длительности продажи
		PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY days_exposition) as median_cnt_days_exp,
		round(avg(days_exposition)::numeric, 2) as avg_days_exp, 
		-- средняя общая площадь и Средняя стоимость кв. метра
		round(avg(total_area)::numeric, 2) as avg_total_area,
		round(avg(last_price/total_area)::numeric) as avg_price_total_area, 
		-- Медиана по кол-ву комнат и средняя по комнатам
		PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY rooms) as median_cnt_room, 
		round(avg(rooms)::numeric, 2) as avg_rooms, 
		-- Медиана по кол-ву балконов и средняя по балконам
		PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY balcony) as median_cnt_balcony, 
		round(avg(balcony)::numeric, 2) as avg_balcony,	
		-- медиана по этажу и средняя по этажу
		PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY floor) as median_floor,
		round(avg(floor)::numeric, 2) as avg_floor
	FROM real_estate.advertisement
	left join real_estate.flats using(id)
	left join real_estate.city using(city_id)
	WHERE id IN (SELECT * FROM filtered_id) and city != 'Санкт-Петербург' and extract(year from first_day_exposition) != 2014 and extract(year from first_day_exposition) != 2019
	group by city
	order by cnt_advertisement desc)
select *
from data_analysis
order by cnt_advertisement desc, perc_cnt_exp desc
limit 15; -- Выбраны города с наибольшим кол-вом объявлений и наибольшей долей. Объявлений со 100% долей продаж крайне мало, по такому кол-ву данных нельзя делать выводы