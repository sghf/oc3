package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// handleGetTags is the common logic for getting tags
func (a *Api) handleGetTags(c echo.Context, tagID *int, query ListQueryParameters) error {
	log := echolog.GetLogHandler(c, "handleGetTags")
	odb := a.ODB
	ctx := c.Request().Context()

	tags, err := odb.GetTags(ctx, tagID, query.Page.Limit, query.Page.Offset)
	if err != nil {
		log.Error("cannot get tags", logkey.TagID, tagID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get tags")
	}

	if tagID != nil {
		// Single tag requested
		if len(tags) == 0 {
			return JSONProblemf(c, http.StatusNotFound, "tag %d not found", *tagID)
		}
		filteredItem, err := filterItemFields(tags[0], query.Props)
		if err != nil {
			log.Error("cannot project tag props", logkey.TagID, *tagID, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot project tag props")
		}
		return c.JSON(http.StatusOK, newDataResponse([]map[string]any{filteredItem}))
	}

	// All tags requested
	filteredItems, err := filterItemsFields(tags, query.Props)
	if err != nil {
		log.Error("cannot project tag props", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot project tag props")
	}
	return c.JSON(http.StatusOK, newListResponse(filteredItems, propsMapping["tag"], query))
}

// GetTags handles GET /tags
func (a *Api) GetTags(c echo.Context, params server.GetTagsParams) error {
	query, err := buildListQueryParameters(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, propsMapping["tag"])
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	log := echolog.GetLogHandler(c, "GetTags")
	log.Info("called", "limit", query.Page.Limit, "offset", query.Page.Offset, "props", query.Props)
	return a.handleGetTags(c, nil, query)
}
