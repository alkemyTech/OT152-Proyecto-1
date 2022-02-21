SELECT university,career,inscription_date,nombre AS names,sexo AS gender,birth_date,location,email 
FROM public.jujuy_utn 
WHERE university='universidad tecnológica nacional' 
AND inscrip-ñtion_date BETWEEN '2020/01/09' AND '2021/02/01'

