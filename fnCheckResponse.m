/*
     Функция проверяет есть ли в вашем аккаунте Яндекс.Директ ссылки на несуществующие страницы.

     Версия 2.3
     -- добавил проверку графических объявлений
     -- поправил баги


     Создатель: Эльдар Забитов (http://zabitov.ru)
*/


let
checkResponse = (token as text, clientlogin as nullable text, findAll as nullable text) =>
let
    // Функция для быстрых ссылок
    fnSitelinkUrl = (sitelinkSetId as text) =>
    let
        sitelinkUrl = "https://api.direct.yandex.com/json/v5/sitelinks",
        sitelinkBody = "{""method"": ""get"",
            ""params"": {
                ""SelectionCriteria"":
                    {
                        ""Ids"": ["""&sitelinkSetId&"""]
                    },
                ""FieldNames"": [""Id"", ""Sitelinks""]
                }
            }",
        sourceSitelinks = Web.Contents(sitelinkUrl,
            [Headers = [
                #"Authorization"=auth,
                #"Accept-Language" = "ru",
                #"Content-Type" = "application/json; charset=utf-8",
                #"Client-Login" = clientLogin],
            Content = Text.ToBinary(sitelinkBody) ]),
        sitelinksJson = Json.Document(sourceSitelinks,65001),
        sitelinksJsonToTable = Record.ToTable(sitelinksJson),
        sitelinksExpand = Table.ExpandRecordColumn(sitelinksJsonToTable, "Value", {"SitelinksSets"}, {"Value.SitelinksSets"}),
        sitelinksExpand1 = Table.ExpandListColumn(sitelinksExpand, "Value.SitelinksSets"),
        sitelinksExpand2 = Table.ExpandRecordColumn(sitelinksExpand1, "Value.SitelinksSets", {"Sitelinks"}, {"Value.SitelinksSets.Sitelinks"}),
        sitelinksExpand3 = Table.ExpandListColumn(sitelinksExpand2, "Value.SitelinksSets.Sitelinks"),
        sitelinksExpand4 = Table.ExpandRecordColumn(sitelinksExpand3, "Value.SitelinksSets.Sitelinks", {"Href"}, {"Value.SitelinksSets.Sitelinks.Href"}),
        sitelinksDelOther = Table.SelectColumns(sitelinksExpand4,{"Value.SitelinksSets.Sitelinks.Href"}),
        sitelinksRenameCol = Table.RenameColumns(sitelinksDelOther,{{"Value.SitelinksSets.Sitelinks.Href", "SitelinkHref"}}),
        delUtmSitelinks = Table.SplitColumn(sitelinksRenameCol, "SitelinkHref", Splitter.SplitTextByDelimiter("?", QuoteStyle.Csv), {"SitelinkHref.1"}),
        changeSitelinksTypeToText = Table.TransformColumnTypes(delUtmSitelinks,{{"SitelinkHref.1", type text}}),
        renameSitelinksCol = Table.RenameColumns(changeSitelinksTypeToText,{{"SitelinkHref.1", "SitelinkHref"}})
    in
        renameSitelinksCol,

    // Функция получения статусов сервера
    fnServerResponse = (urlList as text) =>
    let
        source = Web.Contents(urlList,[ManualStatusHandling={404}]),
        getMetadata = Value.Metadata(source)
    in
        getMetadata,

    // вводные
    clientLogin = if clientlogin = null
        then ""
        else clientlogin,
    findAll = if findAll = "YES"
        then ""
        else ", ""States"": [""ON""]",
    auth = "Bearer "&token,

    // получаем список кампаний в аккаунте и формируем таблицу
    url = "https://api.direct.yandex.com/json/v5/campaigns",
    body = "{""method"": ""get"",
            ""params"": {
                ""SelectionCriteria"":
                    {
                        ""States"": [""ON""]
                        },
                ""FieldNames"": [""Id"", ""Name""]}
        }",
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
                                    ""CampaignIds"": ["""&campaignsId&"""]"&findAll&"
                                },
                                ""FieldNames"": [""Id"", ""State"", ""CampaignId""],
                                ""TextAdFieldNames"": [""Href"", ""SitelinkSetId""],
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
        expandAds1 = Table.ExpandRecordColumn(expandAds, "Ads",
            {"Id", "TextAd", "State", "CampaignId", "TextImageAd"},
            {"Id", "TextAd", "State", "CampaignId", "TextImageAd"}),
        expandHrefLinks = Table.ExpandRecordColumn(expandAds1, "TextAd",
            {"Href", "SitelinkSetId"},
            {"TextHref", "SitelinkSetId"}),
        expandTextImageHref = Table.ExpandRecordColumn(expandHrefLinks, "TextImageAd", {"Href"}, {"TextImageAd.Href"}),
        getOneHref = Table.AddColumn(expandTextImageHref, "Href", each if [TextHref] = null
            then [TextImageAd.Href]
            else [TextHref]),
        delAmotherHref = Table.RemoveColumns(getOneHref,{"TextHref", "TextImageAd.Href"}),
        sitelinkSetIdToText = Table.TransformColumnTypes(delAmotherHref,{{"SitelinkSetId", type text}}),

        duplicateHref = Table.DuplicateColumn(sitelinkSetIdToText, "Href", "HrefClean1"),
        deleteUtm = Table.SplitColumn(duplicateHref,"HrefClean1",Splitter.SplitTextByDelimiter("?", QuoteStyle.Csv),{"HrefClean"}),
        deleteHttps = Table.ReplaceValue(deleteUtm,"https://","",Replacer.ReplaceText,{"HrefClean"}),
        deleteHttp = Table.ReplaceValue(deleteHttps,"http://","",Replacer.ReplaceText,{"HrefClean"}),
        deleteColExHref = Table.SelectColumns(deleteHttp,{"HrefClean"}),
        distinctHref = Table.Distinct(deleteColExHref),
        hrefDelNull = Table.SelectRows(distinctHref, each [HrefClean] <> null),
        hrefFnToTable = Table.AddColumn(hrefDelNull, "Custom", each fnServerResponse([HrefClean])),
        expandHrefResponseStatus = Table.ExpandRecordColumn(hrefFnToTable, "Custom", {"Response.Status"}, {"Response.Status"}),

        deleteColExSitelinks = Table.SelectColumns(deleteHttp,{"SitelinkSetId"}),
        distinctSitelinks = Table.Distinct(deleteColExSitelinks),
        sitelinksDelNull = Table.SelectRows(distinctSitelinks, each [SitelinkSetId] <> null),
        sitelinksFnToTable = Table.AddColumn(sitelinksDelNull, "Custom", each fnSitelinkUrl([SitelinkSetId])),
        expandSitelinks = Table.ExpandTableColumn(sitelinksFnToTable, "Custom", {"SitelinkHref"}, {"SitelinkHref"}),
        expandSitelinksResponse = Table.AddColumn(expandSitelinks, "Пользовательская", each fnServerResponse([SitelinkHref])),
        expandSitelinksResponse1 = Table.ExpandRecordColumn(expandSitelinksResponse, "Пользовательская", {"Response.Status"}, {"Response.Sitelinks"}),

            // запускаем функцию и мерджим статусы URL с списком всех объявлений
        mergeSitelinks = Table.NestedJoin(deleteHttp,{"SitelinkSetId"},expandSitelinksResponse1,{"SitelinkSetId"},"mergeSitelinks",JoinKind.LeftOuter),
        mergeHref = Table.NestedJoin(mergeSitelinks,{"HrefClean"},expandHrefResponseStatus,{"HrefClean"},"mergeHref",JoinKind.LeftOuter),
        expandMergeSitelinks = Table.ExpandTableColumn(mergeHref, "mergeSitelinks",
            {"SitelinkHref", "Response.Sitelinks"},
            {"SitelinkHref", "Response.Sitelinks"}),
        expandMergeHref = Table.ExpandTableColumn(expandMergeSitelinks, "mergeHref", {"Response.Status"}, {"Response.Status"})

    in
        expandMergeHref,

    fnToTable = Table.AddColumn(campaignIdToText, "Custom", each fnCampaignServerResponse([Id])),
    expandFinal = Table.ExpandTableColumn(fnToTable, "Custom",
        {"Name", "Id", "Href", "SitelinkSetId", "State", "CampaignId", "HrefClean", "SitelinkHref", "Response.Sitelinks", "Response.Status"},
        {"Name.1", "AdId", "Href", "SitelinkSetId", "State", "CampaignId", "HrefClean", "SitelinkHref", "Response.Sitelinks", "Response.Status"}),
    checkProblem = Table.AddColumn(expandFinal, "Problem?", each
        if [SitelinkHref] = null and [Response.Status] = 200
            then "No Problem"
            else
            if [Response.Sitelinks] = 200 and [Response.Status] = 200
                    then "No Problem"
                    else "Problem"),
    filterProblem = Table.SelectRows(checkProblem, each ([#"Problem?"] = "Problem")),
    finalStep = Table.SelectColumns(filterProblem,
        {"Name", "AdId", "State", "CampaignId", "HrefClean", "SitelinkHref", "Response.Sitelinks", "Response.Status", "Problem?"})

in
    finalStep
in

checkResponse
