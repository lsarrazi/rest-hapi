const mongoose = require('mongoose')

const Boom = require('@hapi/boom')

function embedsToLevels(embeds) {
  const levels = []

  for (const embed of embeds) {
    const parts = embed.split('.')
    let summedParts = ''

    for (let i = 0; i < parts.length; i++) {
      const part = parts[i]

      if (!levels[i]) levels.push(new Set())

      summedParts += part
      levels[i].add(summedParts)
      summedParts += '.'
    }
  }

  return levels
}

function resolveModelNestedPath(
  baseModel,
  nestedPath,
  nestedPathSplitted = nestedPath.split('.'),
  embedsSet
) {
  let embedSchema
  let subDocumentsCount = 0

  // Try to resolve subdocuments before assuming we want to resolve an object id
  while (subDocumentsCount < nestedPathSplitted.length) {
    const schema = baseModel.schema.path(
      nestedPathSplitted.slice(0, subDocumentsCount + 1).join('.')
    )
    if (!schema) break
    else {
      embedSchema = schema
    }
    subDocumentsCount++
  }

  function throwIfPathEntirelyInQuery(error) {
    if (embedsSet.has(nestedPath)) throw error
  }

  function recreateCurrentPath() {
    return nestedPath
      .split('.')
      .slice(0, subDocumentsCount - nestedPathSplitted.length)
      .join('.')
  }

  if (!embedSchema)
    return throwIfPathEntirelyInQuery(
      Boom.badRequest(
        `cannot embed "${nestedPath}" because "${recreateCurrentPath()}" cannot be found in the model`
      )
    )

  if (embedSchema.instance !== 'ObjectID')
    return throwIfPathEntirelyInQuery(
      Boom.badRequest(
        `cannot embed "${nestedPath}" because "${recreateCurrentPath()}" is not an object id in the model`
      )
    )

  if (typeof embedSchema.options.ref !== 'string')
    return throwIfPathEntirelyInQuery(
      Boom.badRequest(
        `cannot embed "${nestedPath}" because "${recreateCurrentPath()}" object id do not reference any model`
      )
    )

  let embedModel
  try {
    embedModel = mongoose.model(embedSchema.options.ref)
  } catch (e) {
    throw Boom.badRequest(
      `cannot embed "${nestedPath}" because "${recreateCurrentPath()}" object id do reference a model that cannot be found`
    )
  }

  if (nestedPathSplitted.length - subDocumentsCount === 0) return embedModel

  for (let i = 0; i < subDocumentsCount; i++) nestedPathSplitted.shift()

  return resolveModelNestedPath(
    embedModel,
    nestedPath,
    nestedPathSplitted,
    embedsSet
  )
}

function makeLevelAggregation(level, baseModel, basePath, embedsSet) {
  const lookups = []
  const sets = {}

  for (const path of level) {
    const pathModel = resolveModelNestedPath(
      baseModel,
      path,
      undefined,
      embedsSet
    )

    if (!pathModel) continue

    const { collectionName } = pathModel.collection

    const fullPath = basePath ? basePath + '.' + path : path

    lookups.push({
      $lookup: {
        from: collectionName,
        localField: fullPath,
        foreignField: '_id',
        as: fullPath,
      },
    })

    sets[fullPath] = {
      $arrayElemAt: ['$' + fullPath, 0],
    }
  }

  return {
    lookups,
    set: { $set: sets },
  }
}

function makeEmbedsAggregation(query, baseModel, basePath) {
  const embeds = !query.$embed
    ? []
    : Array.isArray(query.$embed)
    ? query.$embed
    : [query.$embed]

  const aggregation = []

  const levels = embedsToLevels(embeds)

  const embedsSet = new Set(embeds)

  for (const level of levels) {
    const { lookups, set } = makeLevelAggregation(
      level,
      baseModel,
      basePath,
      embedsSet
    )

    aggregation.push(...lookups)
    aggregation.push(set)
  }

  return aggregation
}

function makeMatchAggregation(query, basePath) {
  const matchAggregation = []

  const reducer = ($match, property) => {
    const path = basePath ? basePath + '.' + property : property
    $match[path] = query[property]
    return $match
  }

  const matchArray = Object.keys(query).filter(
    (property) => !property.startsWith('$')
  )
  const $match = matchArray.reduce(reducer, {})

  if (matchArray.length !== 0) {
    matchAggregation.push({ $match })
  }

  return matchAggregation
}

function makePaginationAggregation(query) {
  const paginationAggregation = []

  if (query.$skip)
    paginationAggregation.push({
      $skip: query.$skip,
    })

  if (query.$limit)
    paginationAggregation.push({
      $limit: query.$limit,
    })

  return paginationAggregation
}

function makeSortAggregation(query, basePath) {
  const sortAggregation = []

  if (!query.$sort) return []

  const sort = Array.isArray(query.$sort) ? query.$sort : [query.$sort]

  const reducer = (sortObject, criteria) => {
    const negative = criteria.startsWith('-')
    const sliced = criteria.slice(negative ? 1 : 0)
    const path = basePath ? basePath + '.' + sliced : sliced
    sortObject[path] = negative ? -1 : 1
    return sortObject
  }

  const reduced = sort.reduce(reducer, {})

  sortAggregation.push({
    $sort: reduced,
  })

  return sortAggregation
}

function buildAggregationForAssociation(
  query,
  association,
  associationName,
  ownerModel,
  ownerId,
  childModel
) {
  const embedsSet = new Set(Object.keys(query))
  const linkingModel = mongoose.model(association.linkingModel)
  console.log(ownerModel, childModel)
  for (const property of embedsSet) {
    if (!property.startsWith('$')) {
      resolveModelNestedPath(linkingModel, property, undefined, embedsSet)
    }
  }

  const linkingModelFieldNameWhichStoreChildModel =
    association.alias ?? association.model
  const linkingModelFieldPathWhichStoreChildModel = `${associationName}.${linkingModelFieldNameWhichStoreChildModel}`

  const paginationAggregation = makePaginationAggregation(query)

  const sortAggregation = makeSortAggregation(query, associationName)

  const matchAggregation = makeMatchAggregation(query, associationName)

  console.log(matchAggregation)

  const embedsAggregations = makeEmbedsAggregation(
    query,
    mongoose.model(association.linkingModel),
    associationName
  )

  let aggregation = [
    {
      $match: {
        _id: new mongoose.Types.ObjectId(ownerId),
      },
    },
    {
      $lookup: {
        from: association.linkingModel,
        localField: '_id',
        foreignField: ownerModel.collectionName,
        as: associationName,
      },
    },
    {
      $unwind: {
        path: '$' + associationName,
      },
    },
    {
      $lookup: {
        from: childModel.collection.collectionName,
        localField: linkingModelFieldPathWhichStoreChildModel,
        foreignField: '_id',
        as: linkingModelFieldPathWhichStoreChildModel,
      },
    },
    //...embedLookupsArray,
    {
      $set: {
        [associationName]: {
          [linkingModelFieldNameWhichStoreChildModel]: {
            $arrayElemAt: ['$' + linkingModelFieldPathWhichStoreChildModel, 0],
          },
          //...embedSetObject
        },
      },
    },
    ...embedsAggregations,
    ...sortAggregation,
    ...matchAggregation,
    {
      $facet: {
        content: [
          ...paginationAggregation,
          {
            $group: {
              _id: '$_id',
              [associationName]: {
                $push: '$' + associationName,
              },
            },
          },
        ],
        totalCount: [{ $count: 'count' }],
      },
    },
  ]

  console.log('result', JSON.stringify(aggregation, null, '\t'))

  const mongooseQuery = ownerModel.aggregate(aggregation)

  return mongooseQuery
}

function buildAggregationForList(query, ownerModel) {
  const paginationAggregation = makePaginationAggregation(query)

  const embedsAggregations = makeEmbedsAggregation(query, ownerModel)

  const sortAggregation = makeSortAggregation(query)

  const matchAggregation = makeMatchAggregation(query)

  let aggregation = [
    ...embedsAggregations,
    ...sortAggregation,
    ...matchAggregation,
    {
      $facet: {
        content: [...paginationAggregation],
        totalCount: [{ $count: 'count' }],
      },
    },
  ]

  const mongooseQuery = ownerModel.aggregate(aggregation)

  return mongooseQuery
}

module.exports = { buildAggregationForAssociation, buildAggregationForList }
