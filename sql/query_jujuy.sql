SELECT
    university AS universidad,
    career AS carrera,
    inscription_date AS fecha_de_inscripcion,
    nombre,
    sexo,
    birth_date AS fecha_de_nacimiento,
    location AS localidad,
    email
FROM
    public.jujuy_utn
WHERE
    university = 'universidad nacional de jujuy'
    AND inscription_date BETWEEN '2020/09/01' AND '2021/02/01'