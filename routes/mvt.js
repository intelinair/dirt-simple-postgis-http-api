// route query
const sql = (params, query) => {
  return `
    WITH fields as (
        select *
        from ${params.table} test
        join vts.acl on test.fieldid=acl.field_id
        where acl.user_id=${params.userid}
        -- Optional Filter
        ${query.filter ? ` AND ${query.filter}` : ''}
    )
    , mvtgeom as (
      SELECT
        ST_AsMVTGeom (
          ST_Transform(${query.geom_column}, 3857),
          ST_TileEnvelope(${params.z}, ${params.x}, ${params.y})
        ) as geom
        ${query.columns ? `, ${query.columns}` : ''}

      FROM
        fields

      WHERE
        ST_Intersects(
          geometry,
          ST_Transform(
            ST_TileEnvelope(${params.z}, ${params.x}, ${params.y}),
            3857
          )
        )

        -- Optional Filter
    )
    SELECT ST_AsMVT(mvtgeom.*, '${params.table}', 4096, 'geom' ${
    query.id_column ? `, '${query.id_column}'` : ''

  }) AS mvt from mvtgeom;
  `
}

// route schema
const schema = {
  description:
    'Return table as Mapbox Vector Tile (MVT). The layer name returned is the name of the table.',
  tags: ['feature'],
  summary: 'return MVT',
  params: {
    table: {
      type: 'string',
      description: 'The name of the table or view.'
    },
    z: {
      type: 'integer',
      description: 'Z value of ZXY tile.'
    },
    x: {
      type: 'integer',
      description: 'X value of ZXY tile.'
    },
    y: {
      type: 'integer',
      description: 'Y value of ZXY tile.'
    }
  },
  querystring: {
    geom_column: {
      type: 'string',
      description: 'Optional geometry column of the table. The default is geom.',
      default: 'geom'
    },
    columns: {
      type: 'string',
      description:
        'Optional columns to return with MVT. The default is no columns.'
    },
    id_column: {
      type: 'string',
      description:
        'Optional id column name to be used with Mapbox GL Feature State. This column must be an integer a string cast as an integer.'
    },
    filter: {
      type: 'string',
      description: 'Optional filter parameters for a SQL WHERE statement.'
    }
  }
}

// create route
module.exports = function(fastify, opts, next) {
  fastify.route({
    method: 'GET',
    url: '/mvt/:table/:userid/:z/:x/:y',
    schema: schema,
    handler: function(request, reply) {
      fastify.pg.connect(onConnect)

      function onConnect(err, client, release) {
        if (err)
          return reply.send({
            statusCode: 500,
            error: 'Internal Server Error',
            message: 'unable to connect to database server'
          })
        console.log(request.params);
        client.query(sql(request.params, request.query), function onResult(
          err,
          result
        ) {
          release()
          if (err) {
            reply.send(err)
          } else {
            const mvt = result.rows[0].mvt
            if (mvt.length === 0) {
              reply.code(204)
            }
            reply.header('Content-Type', 'application/x-protobuf').send(mvt)
          }
        })
      }
    }
  })
  next()
}

module.exports.autoPrefix = '/v1'
