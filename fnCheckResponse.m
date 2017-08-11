/*
     Функция проверяет есть ли в вашем аккаунте Яндекс.Директ ссылки на несуществующие страницы.

     Версия 1.1
     -- добавлена возможность поиска по активным и не активным объявлениям


     Создатель: Эльдар Забитов (http://zabitov.ru)
*/


let
checkResponse = (token as text, clientlogin as nullable text, findAll as nullable text) =>
let

    // вводные
    clientLogin = if clientlogin = null then "" else clientlogin,
    findAll = if findAll = "YES" then "" else ", ""States"": [""ON""]",
    auth = "Bearer "&token,

    // получаем список кампаний в аккаунте и формируем таблицу
    url = "https://api.direct.yandex.com/json/v5/campaigns",
    body = "{""method"": ""get"", ""params"": {""SelectionCriteria"": {}, ""FieldNames"": [""Id"", ""Name""]
                   }}",
    userIdSource = Web.Contents(url,
        [Headers = [#"Authorization"=auth,
                    #"Accept-Language" = "ru",
                    #"Content-Type" = "application/json; charset=utf-8",
                    #"Client-Login" = clientLogin],
        Content = Text.ToBinary(body) ]),
    jsonList = Json.Document(userIdSource,65001),
    campaignToTable = Record.ToTable(jsonList),
    deleteNameColumn = Table.RemoveColumns(campaignToTable,{"Name"}),
    expandValueCampaign = Table.ExpandRecordColumn(deleteNameColumn, "Value", {"Campaigns"}, {"Campaigns"}),
    expandCampaign = Table.ExpandListColumn(expandValueCampaign, "Campaigns"),
    expandCampaign1 = Table.ExpandRecordColumn(expandCampaign, "Campaigns", {"Id", "Name"}, {"Id", "Name"}),
    campaignIdToText = Table.TransformColumnTypes(expandCampaign1,{{"Id", type text}}),

    // функция для сбора включенных объявлений и их ссылок
    fnCampaignServerResponse = (campaignsId as text) =>
    let
        urlAds = "https://api.direct.yandex.com/json/v5/ads",
        bodyAds = "{""method"":
                    ""get"",
                        ""params"":
                            {""SelectionCriteria"":
                                {
                                    ""CampaignIds"": ["""&campaignsId&"""]"&findAll&

                                "},
                                ""FieldNames"": [""Id"", ""State"", ""CampaignId""],
                                ""TextAdFieldNames"": [""Href""],
                                ""TextImageAdFieldNames"": [""Href""]
                                }
                    }",
        getAds = Web.Contents(urlAds,
            [Headers = [#"Authorization"=auth,
                        #"Accept-Language" = "ru",
                        #"Content-Type" = "application/json; charset=utf-8",
                        #"Client-Login" = clientLogin],
            Content = Text.ToBinary(bodyAds) ]),
        jsonListAds = Json.Document(getAds,65001),
        jsonToTableAds = Record.ToTable(jsonListAds),
        expandValueAds = Table.ExpandRecordColumn(jsonToTableAds, "Value", {"Ads"}, {"Ads"}),
        expandAds = Table.ExpandListColumn(expandValueAds, "Ads"),
        expandAds1 = Table.ExpandRecordColumn(expandAds, "Ads", {"Id", "TextAd", "Status", "CampaignId"}, {"Id", "TextAd", "Status", "CampaignId"}),
        expandHref = Table.ExpandRecordColumn(expandAds1, "TextAd", {"Href"}, {"Href"}),
        duplicateHref = Table.DuplicateColumn(expandHref, "Href", "HrefClean1"),
        deleteUtm = Table.SplitColumn(duplicateHref,"HrefClean1",Splitter.SplitTextByDelimiter("?", QuoteStyle.Csv),{"HrefClean"}),
        deleteHttps = Table.ReplaceValue(deleteUtm,"https://","",Replacer.ReplaceText,{"HrefClean"}),
        deleteHttp = Table.ReplaceValue(deleteHttps,"http://","",Replacer.ReplaceText,{"HrefClean"}),
        deleteAnotherColumns = Table.SelectColumns(deleteHttp,{"HrefClean"}),

        // удаляем дубли чтоб уменьшить нагрузку
        distinctHref = Table.Distinct(deleteAnotherColumns),

        // функция получения статусов сервера
        fnServerResponse = (urlList as text) =>
        let
            Source = Web.Contents(urlList,[ManualStatusHandling={404}]),
            GetMetadata = Value.Metadata(Source)
        in
            GetMetadata,

        // запускаем функцию и мерджим статусы URL с списком всех объявлений
        getFnToTable = Table.AddColumn(distinctHref, "Custom", each fnServerResponse([HrefClean])),
        expandResponseStatus = Table.ExpandRecordColumn(getFnToTable, "Custom", {"Response.Status"}, {"Response.Status"}),
        mergeToAllCampaign = Table.NestedJoin(deleteHttp,{"HrefClean"},expandResponseStatus,{"HrefClean"},"NewColumn",JoinKind.LeftOuter),
        expandeResponseStatus = Table.ExpandTableColumn(mergeToAllCampaign, "NewColumn", {"Response.Status"}, {"Response.Status"})
    in
        expandeResponseStatus,

    // формируем итоговую таблицу
    addResponseToTable = Table.AddColumn(campaignIdToText, "Custom", each fnCampaignServerResponse([Id])),
    expandFinal = Table.ExpandTableColumn(addResponseToTable, "Custom", {"Id", "Href", "HrefClean", "Response.Status"}, {"AdId", "Href", "HrefClean", "Response.Status"}),
    renameCampaignId = Table.RenameColumns(expandFinal,{{"Id", "CampaignId"}}),

    // удаляем объявления с пустым полем ссылок и статусом не 200
    filterEmptyHref = Table.SelectRows(renameCampaignId, each ([Href] <> null)),
    filterNonTwoHundred = Table.SelectRows(filterEmptyHref, each ([Response.Status] <> 200))
in
    filterNonTwoHundred
in
    checkResponse
