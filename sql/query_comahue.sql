SELECT  universidad AS university,
        carrera AS career,
        fecha_de_inscripcion AS inscription_date,
        name,
        sexo AS sex,
        fecha_nacimiento AS birth_date,
        codigo_postal AS zip_code,
        correo_electronico AS email
FROM public.flores_comahue
WHERE universidad = 'UNIV. NACIONAL DEL COMAHUE'
AND fecha_de_inscripcion BETWEEN '2020-09-1' AND '2021-02-01';