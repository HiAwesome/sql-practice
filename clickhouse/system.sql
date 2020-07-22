SELECT
    partition,
    name,
    active
FROM system.parts
WHERE table = 'visits';
