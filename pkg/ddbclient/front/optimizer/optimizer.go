package optimizer

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"

	"github.com/AgentGuo/ddb/pkg/ddbclient/front/plan"
)

func Optimize(ppt plan.Plantree) plan.Plantree {
	predPushDown(&ppt)
	prune(&ppt)
	transferOptimize(&ppt)
	projPushDown(&ppt)
	return ppt
}

func predPushDown(ppt *plan.Plantree) {
	//如果是scan或者union，就没有predicate了，就不下推了
	if ppt.Root.OperType == plan.Predicate {
		pred := ppt.Root
		predCondi := pred.PredicateOper.PredConditions
		travelTreeInPD(pred, &predCondi)
		if ppt.Root.Childs[0].OperType != plan.Scan {
			ppt.Root = ppt.Root.Childs[0]
		}
	} else if ppt.Root.Childs[0].OperType == plan.Predicate {
		pred := ppt.Root.Childs[0]
		predCondi := pred.PredicateOper.PredConditions
		travelTreeInPD(pred, &predCondi)
		if ppt.Root.Childs[0].Childs[0].OperType != plan.Scan {
			ppt.Root.Childs[0] = ppt.Root.Childs[0].Childs[0]
		}
	}
}

// 目前不考虑Join的Union子节点或Scan子节点被剪掉，不考虑Union的所有Scan子节点全部被剪掉
// 目前不考虑condition里有自表比较
func travelTreeInPD(root *plan.Operator_, predcondi *[]plan.ConditionUnit_) {
	for i := range root.Childs {
		if root.Childs[i].OperType == plan.Join {
			travelTreeInPD(root.Childs[i], predcondi)
		} else if root.Childs[i].OperType == plan.Union {
			unusedChilds := []int{}
			travelTreeInPD(root.Childs[i], predcondi)
			for j := range root.Childs[i].Childs {
				if root.Childs[i].Childs[j].Unused {
					unusedChilds = append(unusedChilds, j)
				}
			}
			for id, cnt := range unusedChilds {
				root.Childs[i].Childs = append(root.Childs[i].Childs[:cnt-id], root.Childs[i].Childs[cnt-id+1:]...)
			}
		} else if root.Childs[i].OperType == plan.Scan {
			scan := root.Childs[i]
			if !scan.ScanOper.Frag.IsVertical {
				for id, j := range *predcondi {
					if j.Lexpression.IsField && !j.Rexpression.IsField {
						predPrune(&(*predcondi)[id], scan, &j.Lexpression, &j.Rexpression)
					} else if !j.Lexpression.IsField && j.Rexpression.IsField {
						predPrune(&(*predcondi)[id], scan, &j.Rexpression, &j.Lexpression)
					}
					if scan.Unused {
						break
					}
				}
			}
			if !scan.Unused {
				pred := plan.Operator_{}
				pred.OperType = plan.Predicate
				pred.PredicateOper = &plan.PredicateOper_{}
				for id, j := range *predcondi {
					if j.Lexpression.IsField && !j.Rexpression.IsField {
						if scan.ScanOper.Frag.IsVertical {
							if j.Lexpression.Field.TableName == scan.ScanOper.TableName {
								for k := range scan.ScanOper.Frag.Cols {
									if j.Lexpression.Field.FieldName == scan.ScanOper.Frag.Cols[k] {
										pred.PredicateOper.PredConditions = append(pred.PredicateOper.PredConditions, (*predcondi)[id])
										break
									}
								}
							}
						} else {
							if j.Lexpression.Field.TableName == scan.ScanOper.TableName {
								pred.PredicateOper.PredConditions = append(pred.PredicateOper.PredConditions, (*predcondi)[id])
							}
						}
					} else if !j.Lexpression.IsField && j.Rexpression.IsField {
						if scan.ScanOper.Frag.IsVertical {
							if j.Rexpression.Field.TableName == scan.ScanOper.TableName {
								for k := range scan.ScanOper.Frag.Cols {
									if j.Rexpression.Field.FieldName == scan.ScanOper.Frag.Cols[k] {
										pred.PredicateOper.PredConditions = append(pred.PredicateOper.PredConditions, (*predcondi)[id])
										break
									}
								}
							}
						} else {
							if j.Rexpression.Field.TableName == scan.ScanOper.TableName {
								pred.PredicateOper.PredConditions = append(pred.PredicateOper.PredConditions, (*predcondi)[id])
							}
						}
					}
				}

				if len(pred.PredicateOper.PredConditions) != 0 {
					// pred.Parent = root
					root.Childs[i] = &pred

					pred.Childs = append(pred.Childs, scan)
					// scan.Parent = &pred
				}
			}
		} else {
			fmt.Println("error in travelTree")
		}
	}
}

func predPrune(j *plan.ConditionUnit_, scan *plan.Operator_, Lexpression *plan.Expression_, Rexpression *plan.Expression_) {
	unnumber := regexp.MustCompile(`[^0-9]`)

	// if len(out) != 0 {
	// 	fmt.Println("djs")
	// 	fmt.Printf("out: %v\n", out)
	// }

	if Lexpression.Field.TableName == scan.ScanOper.TableName {
		for _, k := range scan.ScanOper.Frag.Condition {
			//假设分片条件常量在右边
			if Lexpression.Field.FieldName == k.Lexpression.Field.FieldName {
				s1 := unnumber.FindAllString(string(Rexpression.Value), -1)
				s2 := unnumber.FindAllString(string(k.Rexpression.Value), -1)
				if len(s1) == 0 && len(s2) == 0 {
					v1, _ := strconv.Atoi(string(Rexpression.Value))
					v2, _ := strconv.Atoi(string(k.Rexpression.Value))
					switch j.CompOp {
					case plan.Lt:
						{
							switch k.CompOp {
							//目前不考虑double
							case plan.Gt, plan.Ge, plan.Eq:
								{
									//目前不考虑double
									if v1 <= v2 {
										scan.Unused = true
										break
									}
								}
							}
						}
					case plan.Le:
						{
							switch k.CompOp {
							//目前不考虑double
							case plan.Gt:
								{
									//目前不考虑double
									if v1 <= v2 {
										scan.Unused = true
										break
									}
								}

							case plan.Ge, plan.Eq:
								{
									//目前不考虑double
									if v1 < v2 {
										scan.Unused = true
										break
									}
								}
							}
						}
					case plan.Eq:
						{
							switch k.CompOp {
							//目前不考虑double
							case plan.Gt:
								{
									//目前不考虑double
									if v1 <= v2 {
										scan.Unused = true
										break
									}
								}

							case plan.Ge:
								{
									//目前不考虑double
									if v1 < v2 {
										scan.Unused = true
										break
									}
								}
							case plan.Lt:
								{
									//目前不考虑double
									if v1 >= v2 {
										scan.Unused = true
										break
									}
								}
							case plan.Le:
								{
									//目前不考虑double
									if v1 > v2 {
										scan.Unused = true
										break
									}
								}
							case plan.Eq:
								{
									//目前不考虑double
									if v1 != v2 {
										scan.Unused = true
										break
									}
								}
							case plan.Neq:
								{
									if v1 == v2 {
										scan.Unused = true
										break
									}
								}
							}
						}
					case plan.Ge:
						{
							switch k.CompOp {
							//目前不考虑double
							case plan.Lt:
								{
									//目前不考虑double
									if v1 >= v2 {
										scan.Unused = true
										break
									}
								}

							case plan.Le, plan.Eq:
								{
									//目前不考虑double
									if v1 > v2 {
										scan.Unused = true
										break
									}
								}
							}
						}
					case plan.Gt:
						{
							switch k.CompOp {
							//目前不考虑double
							case plan.Lt, plan.Le, plan.Eq:
								{
									//目前不考虑double
									if v1 >= v2 {
										scan.Unused = true
										break
									}
								}
							}
						}
					case plan.Neq:
						{
							switch k.CompOp {
							//目前不考虑double
							case plan.Eq:
								{
									//目前不考虑double
									if v1 == v2 {
										scan.Unused = true
										break
									}
								}
							}
						}
					}
				} else {
					switch j.CompOp {
					case plan.Eq:
						{
							switch k.CompOp {
							case plan.Eq:
								{
									if string(Rexpression.Value) != string(k.Rexpression.Value) {
										scan.Unused = true
										break
									}
								}
							case plan.Neq:
								{
									if string(Rexpression.Value) == string(k.Rexpression.Value) {
										scan.Unused = true
										break
									}
								}
							}

						}
					case plan.Neq:
						{
							switch k.CompOp {
							case plan.Eq:
								{
									if string(Rexpression.Value) == string(k.Rexpression.Value) {
										scan.Unused = true
										break
									}
								}

							}
						}
					}

				}
			}
		}
	}
}

func prune(ppt *plan.Plantree) {

}
func transferOptimize(ppt *plan.Plantree) {
	travelTreeInTO(ppt.Root)
}
func travelTreeInTO(root *plan.Operator_) {
	if root.OperType == plan.Union {
		for i := range root.Childs {
			if root.Childs[i].OperType == plan.Predicate {
				root.Childs[i].Site = root.Childs[i].Childs[0].Site
				root.Childs[i].DestSite = root.Childs[0].Childs[0].Site
				if root.Childs[i].Site != root.Childs[i].DestSite {
					root.Childs[i].NeedTransfer = true
				}
			} else if root.Childs[i].OperType == plan.Scan {
				root.Childs[i].DestSite = root.Childs[0].Site
				if root.Childs[i].Site != root.Childs[i].DestSite {
					root.Childs[i].NeedTransfer = true
				}
			} else {
				fmt.Println("error in travelTreeInTO")
			}
		}
		root.Site = root.Childs[0].Site
	} else if root.OperType == plan.Predicate {
		travelTreeInTO(root.Childs[0])
		root.Site = root.Childs[0].Site
	} else if root.OperType == plan.Project {
		travelTreeInTO(root.Childs[0])
		root.Site = root.Childs[0].Site
	} else if root.OperType == plan.Join {
		travelTreeInTO(root.Childs[0])
		travelTreeInTO(root.Childs[1])
		root.Childs[1].DestSite = root.Childs[0].Site
		if root.Childs[1].Site != root.Childs[1].DestSite {
			root.Childs[1].NeedTransfer = true
		}
		root.Site = root.Childs[0].Site
	}
}
func projPushDown(ppt *plan.Plantree) {
	if ppt.Root.OperType == plan.Project {
		travelTreeInPDProj(ppt.Root.Childs[0], &ppt.Root.ProjectOper.Fields)
		if ppt.Root.Childs[0].OperType == plan.Union {
			ppt.Root = ppt.Root.Childs[0]
		} else if ppt.Root.Childs[0].OperType == plan.Predicate && ppt.Root.Childs[0].Childs[0].OperType == plan.Union {
			ppt.Root = ppt.Root.Childs[0]
		}
	} else {
		// newProj0 := plan.Operator_{}
		// newProj0.OperType = plan.Project
		// newProj0.ProjectOper = &plan.ProjectOper_{}
		// travelTreeInPDProj(ppt.Root, &newProj0.ProjectOper.Fields)
	}

}

// 默认Join是单条件的
func travelTreeInPDProj(root *plan.Operator_, proj *[]plan.Field_) {

	v, _ := json.Marshal(proj)
	if root.OperType == plan.Join {
		//left
		if root.Childs[0].OperType == plan.Join {
			newProj0 := plan.Operator_{}
			newProj0.OperType = plan.Project
			newProj0.ProjectOper = &plan.ProjectOper_{}
			json.Unmarshal(v, &newProj0.ProjectOper.Fields)
			newProj0.ProjectOper.Fields = append(newProj0.ProjectOper.Fields, root.JoinOper.JoinConditions[0].Lexpression.Field)

			root.Childs[0] = &newProj0
			// newProj0.Parent = root

			// root.Childs[0].Parent = &newProj0
			newProj0.Childs = append(newProj0.Childs, root.Childs[0])
			newProj0.Site = newProj0.Childs[0].Site

			travelTreeInPDProj(root.Childs[0].Childs[0], &newProj0.ProjectOper.Fields)
			// travelTreeInPDProj(root.Childs[1], proj)
		} else if root.Childs[0].OperType == plan.Union {
			for id := range root.Childs[0].Childs {
				newProj0 := plan.Operator_{}
				newProj0.OperType = plan.Project
				newProj0.ProjectOper = &plan.ProjectOper_{}
				json.Unmarshal(v, &newProj0.ProjectOper.Fields)
				newProj0.ProjectOper.Fields = append(newProj0.ProjectOper.Fields, root.JoinOper.JoinConditions[0].Lexpression.Field)

				// root.Childs[0].Childs[id].Parent = &newProj0
				newProj0.Childs = append(newProj0.Childs, root.Childs[0].Childs[id])

				root.Childs[0].Childs[id] = &newProj0
				// newProj0.Parent = root.Childs[0]

				newProj0.Site = newProj0.Childs[0].Site
			}
		} else if root.Childs[0].OperType == plan.Scan || root.Childs[0].OperType == plan.Predicate {
			newProj0 := plan.Operator_{}
			newProj0.OperType = plan.Project
			newProj0.ProjectOper = &plan.ProjectOper_{}
			json.Unmarshal(v, &newProj0.ProjectOper.Fields)
			newProj0.ProjectOper.Fields = append(newProj0.ProjectOper.Fields, root.JoinOper.JoinConditions[0].Lexpression.Field)

			// root.Childs[0].Parent = &newProj0
			newProj0.Childs = append(newProj0.Childs, root.Childs[0])

			root.Childs[0] = &newProj0
			// newProj0.Parent = root

			newProj0.Site = newProj0.Childs[0].Site
		} else {
			fmt.Println("error in travelTreeInPDProj")
		}

		//right
		if root.Childs[1].OperType == plan.Join {
			newProj1 := plan.Operator_{}
			newProj1.OperType = plan.Project
			newProj1.ProjectOper = &plan.ProjectOper_{}
			json.Unmarshal(v, &newProj1.ProjectOper.Fields)
			newProj1.ProjectOper.Fields = append(newProj1.ProjectOper.Fields, root.JoinOper.JoinConditions[0].Rexpression.Field)

			root.Childs[1] = &newProj1
			// newProj1.Parent = root

			// root.Childs[1].Parent = &newProj1
			newProj1.Childs = append(newProj1.Childs, root.Childs[1])
			newProj1.Site = newProj1.Childs[0].Site
			travelTreeInPDProj(root.Childs[1].Childs[0], &newProj1.ProjectOper.Fields)
		} else if root.Childs[1].OperType == plan.Union {
			for id := range root.Childs[1].Childs {
				newProj0 := plan.Operator_{}
				newProj0.OperType = plan.Project
				newProj0.ProjectOper = &plan.ProjectOper_{}
				json.Unmarshal(v, &newProj0.ProjectOper.Fields)
				newProj0.ProjectOper.Fields = append(newProj0.ProjectOper.Fields, root.JoinOper.JoinConditions[0].Rexpression.Field)

				// root.Childs[1].Childs[id].Parent = &newProj0
				newProj0.Childs = append(newProj0.Childs, root.Childs[1].Childs[id])

				root.Childs[1].Childs[id] = &newProj0
				// newProj0.Parent = root.Childs[1]

				newProj0.Site = newProj0.Childs[0].Site
			}
		} else if root.Childs[1].OperType == plan.Scan || root.Childs[1].OperType == plan.Predicate {
			newProj0 := plan.Operator_{}
			newProj0.OperType = plan.Project
			newProj0.ProjectOper = &plan.ProjectOper_{}
			json.Unmarshal(v, &newProj0.ProjectOper.Fields)
			newProj0.ProjectOper.Fields = append(newProj0.ProjectOper.Fields, root.JoinOper.JoinConditions[0].Rexpression.Field)

			// root.Childs[1].Parent = &newProj0
			newProj0.Childs = append(newProj0.Childs, root.Childs[1])

			root.Childs[1] = &newProj0
			// newProj0.Parent = root

			newProj0.Site = newProj0.Childs[0].Site
		} else {
			fmt.Println("error in travelTreeInPDProj")
		}

	} else if root.OperType == plan.Predicate {
		travelTreeInPDProj(root.Childs[0], proj)
	} else if root.OperType == plan.Union {
		for id := range root.Childs {
			newProj0 := plan.Operator_{}
			newProj0.OperType = plan.Project
			newProj0.ProjectOper = &plan.ProjectOper_{}
			json.Unmarshal(v, &newProj0.ProjectOper.Fields)

			// root.Childs[0].Childs[id].Parent = &newProj0
			newProj0.Childs = append(newProj0.Childs, root.Childs[id])

			root.Childs[id] = &newProj0
			// newProj0.Parent = root.Childs[0]

			newProj0.Site = newProj0.Childs[0].Site
		}
	} else if root.OperType == plan.Scan {
		fmt.Print("")
	}

}
