SELECT
    universidad,
    careers AS carrera,
    fecha_de_inscripcion,
    names AS nombre,
    sexo,
    birth_dates AS fecha_de_nacimiento,
    codigo_postal,
    correos_electronicos AS email
FROM
    public.palermo_tres_de_febrero
WHERE
    universidad = '_universidad_de_palermo'
    AND TO_DATE(fecha_de_inscripcion, 'DD/Mon/YY') BETWEEN '01/Sep/20' AND '01/Feb/21'