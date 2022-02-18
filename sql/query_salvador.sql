SELECT  universidad AS university,
        carrera AS career,
        fecha_de_inscripcion AS inscription_date,
        nombre AS name,
        sexo AS sex,
        fecha_nacimiento AS birth_date,
        localidad AS location,
        email
FROM public.salvador_villa_maria
WHERE universidad = 'UNIVERSIDAD_DEL_SALVADOR'
AND TO_DATE(fecha_de_inscripcion,'DD/Mon/YY') between '01/Sep/20' and '01/Feb/21';